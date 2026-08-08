//! Storage Tier Lifecycle Worker
//!
//! Background worker that automatically transitions data between storage tiers
//! based on configured tier policies. Runs periodically (default: every hour).
//!
//! **Age-based lifecycle policies** (`StorageTierPolicy::Lifecycle`):
//! For each source with an age-based policy, the worker:
//! 1. Lists all committed partitions
//! 2. Calculates the age of each partition from `partition_date`
//! 3. Determines the target tier based on lifecycle transitions (sorted by `after_days` descending)
//! 4. If the current tier differs from the target, publishes a Kafka job to trigger the transition
//! 5. Updates `current_tier` and `last_tier_evaluation_at` on the partition row
//!
//! **Access-based policies** (`StorageTierPolicy::AccessBased`):
//! For each source with an access-based policy, the worker:
//! 1. Counts queries in `source_access_log` within the sensitivity's evaluation window
//! 2. If the count exceeds the promote threshold, promotes the source one tier hotter
//! 3. If the count is below the demote threshold, demotes the source one tier colder
//! 4. Publishes a Kafka job for the tier transition
//!
//! **Cleanup**: Deletes `source_access_log` rows older than the maximum evaluation
//! window (30 days) to prevent unbounded table growth.

use chrono::Utc;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::kafka::{KafkaProducer, SyncJobKafkaMessage};
use crate::warehouse::sources::types::{
    AccessSensitivity, StorageTier, StorageTierPolicy, TierTransition,
};
use crate::warehouse::types::JobType;

/// Abstraction over the Kafka send operation so the lifecycle evaluation
/// logic can be tested with a mock sender instead of a real broker.
#[async_trait::async_trait]
pub trait SyncJobSender: Send + Sync {
    async fn send_sync_job(&self, message: &SyncJobKafkaMessage) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl SyncJobSender for KafkaProducer {
    async fn send_sync_job(&self, message: &SyncJobKafkaMessage) -> anyhow::Result<()> {
        KafkaProducer::send_sync_job(self, message).await
    }
}

#[async_trait::async_trait]
impl<S: SyncJobSender> SyncJobSender for Arc<S> {
    async fn send_sync_job(&self, message: &SyncJobKafkaMessage) -> anyhow::Result<()> {
        (**self).send_sync_job(message).await
    }
}

/// Configuration for the lifecycle worker.
#[derive(Debug, Clone)]
pub struct LifecycleWorkerConfig {
    /// How often to evaluate partition tiers.
    pub interval: Duration,
}

impl Default for LifecycleWorkerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Background worker that evaluates and transitions partition storage tiers.
///
/// This worker runs periodically and:
/// 1. Finds sources with lifecycle tier policies
/// 2. Evaluates each partition against the lifecycle transitions
/// 3. Publishes Kafka jobs for any tier transitions needed
pub struct LifecycleWorker {
    db: PgPool,
    kafka: Arc<KafkaProducer>,
    config: LifecycleWorkerConfig,
    shutdown_tx: watch::Sender<bool>,
}

impl LifecycleWorker {
    /// Create a new lifecycle worker.
    pub fn new(
        db: PgPool,
        kafka: Arc<KafkaProducer>,
        config: LifecycleWorkerConfig,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            db,
            kafka,
            config,
            shutdown_tx,
        }
    }

    /// Create a new lifecycle worker with default configuration.
    pub fn with_defaults(db: PgPool, kafka: Arc<KafkaProducer>) -> Self {
        Self::new(db, kafka, LifecycleWorkerConfig::default())
    }

    /// Start the lifecycle worker.
    ///
    /// Returns a JoinHandle for the worker task. The caller should await
    /// this handle or add it to a join set for proper shutdown coordination.
    pub fn start(&mut self) -> JoinHandle<()> {
        let db = self.db.clone();
        let kafka = self.kafka.clone();
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.interval);

            info!(
                interval_secs = config.interval.as_secs(),
                "Storage tier lifecycle worker started"
            );

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = evaluate_tiers(&db, &kafka).await {
                            error!(error = %e, "Failed to evaluate storage tier lifecycles");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            info!("Storage tier lifecycle worker shutting down");
                            return;
                        }
                    }
                }
            }
        });

        handle
    }

    /// Signal the worker to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Get the shutdown receiver for external coordination.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Manually trigger a tier evaluation.
    ///
    /// Useful for testing or forcing an immediate evaluation.
    #[tracing::instrument(
        name = "warehouse.lifecycle.trigger_evaluate",
        skip_all,
        err(Display),
    )]
    pub async fn trigger_evaluate(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        evaluate_tiers(&self.db, &self.kafka).await
    }
}

/// A source with a lifecycle tier policy.
struct LifecycleSource {
    id: Uuid,
    project_id: Uuid,
    name: String,
    transitions: Vec<TierTransition>,
}

/// A partition to evaluate for tier transitions.
struct PartitionInfo {
    id: Uuid,
    table_name: String,
    partition_date: chrono::NaiveDate,
    current_tier: StorageTier,
}

/// Evaluate all sources with lifecycle or access-based policies and transition as needed.
///
/// Returns the number of tier transitions published.
#[tracing::instrument(name = "pond.lifecycle.evaluate_tiers", skip(db, sender))]
async fn evaluate_tiers<S: SyncJobSender>(
    db: &PgPool,
    sender: &S,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut total_transitions: u64 = 0;

    // --- Age-based lifecycle policies (per-partition) ---
    let lifecycle_sources = find_lifecycle_sources(db).await?;

    if !lifecycle_sources.is_empty() {
        info!(
            source_count = lifecycle_sources.len(),
            "Evaluating age-based storage tier lifecycles"
        );

        for source in &lifecycle_sources {
            match evaluate_source_partitions(db, sender, source).await {
                Ok(count) => {
                    total_transitions += count;
                }
                Err(e) => {
                    error!(
                        source_id = %source.id,
                        source_name = %source.name,
                        error = %e,
                        "Failed to evaluate partitions for source"
                    );
                }
            }
        }
    }

    // --- Access-based policies (per-source) ---
    let access_sources = find_access_based_sources(db).await?;

    if !access_sources.is_empty() {
        info!(
            source_count = access_sources.len(),
            "Evaluating access-based storage tier policies"
        );

        for source in &access_sources {
            match evaluate_access_based_source(db, sender, source).await {
                Ok(transitioned) => {
                    if transitioned {
                        total_transitions += 1;
                    }
                }
                Err(e) => {
                    error!(
                        source_id = %source.id,
                        source_name = %source.name,
                        error = %e,
                        "Failed to evaluate access-based policy for source"
                    );
                }
            }
        }
    }

    // --- Cleanup old access log rows ---
    if let Err(e) = cleanup_old_access_logs(db).await {
        warn!(error = %e, "Failed to clean up old source_access_log rows");
    }

    if total_transitions > 0 {
        info!(
            transitions = total_transitions,
            "Published tier transition jobs"
        );
    }

    Ok(total_transitions)
}

/// Find all enabled sources with lifecycle tier policies.
#[tracing::instrument(
    name = "warehouse.lifecycle.find_lifecycle_sources",
    skip_all,
    err(Display),
)]
async fn find_lifecycle_sources(
    db: &PgPool,
) -> Result<Vec<LifecycleSource>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, name, storage_tier_policy
        FROM warehouse_sources
        WHERE storage_tier_policy->>'type' = 'lifecycle'
          AND enabled = true
        "#,
    )
    .fetch_all(db)
    .await?;

    let mut sources = Vec::with_capacity(rows.len());

    for row in rows {
        let id: Uuid = row.get("id");
        let project_id: Uuid = row.get("project_id");
        let name: String = row.get("name");
        let policy_json: serde_json::Value = row.get("storage_tier_policy");

        // Parse the storage tier policy
        let policy: StorageTierPolicy = match serde_json::from_value(policy_json.clone()) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    source_id = %id,
                    error = %e,
                    policy = %policy_json,
                    "Failed to parse storage tier policy, skipping source"
                );
                continue;
            }
        };

        // Extract transitions from lifecycle policy
        if let StorageTierPolicy::Lifecycle { transitions } = policy {
            sources.push(LifecycleSource {
                id,
                project_id,
                name,
                transitions,
            });
        }
    }

    Ok(sources)
}

/// Evaluate all partitions for a single source and publish transition jobs.
///
/// Returns the number of transitions published.
#[tracing::instrument(name = "pond.lifecycle.evaluate_source_partitions", skip(db, sender, source), fields(source_id = %source.id))]
async fn evaluate_source_partitions<S: SyncJobSender>(
    db: &PgPool,
    sender: &S,
    source: &LifecycleSource,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    // Fetch committed partitions for this source
    let partitions = find_source_partitions(db, source.id).await?;

    if partitions.is_empty() {
        return Ok(0);
    }

    let today = Utc::now().date_naive();
    let mut transition_count: u64 = 0;

    for partition in &partitions {
        // Calculate partition age in days
        let age_days = (today - partition.partition_date).num_days();
        if age_days < 0 {
            // Future partition date, skip
            continue;
        }
        let age_days = age_days as u32;

        // Determine target tier by walking transitions sorted by after_days descending.
        // The first matching rule (highest after_days that the partition exceeds) wins.
        let target_tier = determine_target_tier(&source.transitions, age_days);

        let Some(target_tier) = target_tier else {
            // No matching transition rule — partition is younger than all thresholds.
            // Leave it in its current tier.
            continue;
        };

        // Compare current tier to target tier
        if partition.current_tier == target_tier {
            // Already at the correct tier, just update evaluation timestamp
            update_tier_evaluation(db, partition.id).await?;
            continue;
        }

        // Determine the job type and the actual next tier for this transition.
        let Some((job_type, actual_next_tier)) =
            determine_job_type(&partition.current_tier, &target_tier)
        else {
            // Same tier or unexpected combination, skip
            continue;
        };

        tracing::debug!(
            source_id = %source.id,
            partition_id = %partition.id,
            table_name = %partition.table_name,
            current_tier = %partition.current_tier,
            target_tier = %target_tier,
            age_days = age_days,
            job_type = %job_type,
            "Tier transition needed for partition"
        );

        // Use a transaction for the DB operations so they can be rolled back
        // if the Kafka publish fails.
        let mut tx = db.begin().await?;

        // Atomic dedup+insert: only create a job if no pending/running
        // job exists for this source. Avoids the TOCTOU race of check-then-insert.
        let job_id = Uuid::new_v4();
        let insert_result = sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
             SELECT $1, $2, $3, 'pending', NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM warehouse_jobs
                 WHERE source_id = $3
                   AND job_type = $2
                   AND status IN ('pending', 'running')
             )",
        )
        .bind(job_id)
        .bind(job_type.to_string())
        .bind(source.id)
        .execute(&mut *tx)
        .await?;

        if insert_result.rows_affected() == 0 {
            tx.rollback().await?;
            info!(
                source_id = %source.id,
                job_type = %job_type,
                "Skipping transition, active job already exists for source"
            );
            continue;
        }

        // Mark the partition as being evaluated (prevents re-evaluation) but keep
        // the current tier unchanged until the transition job completes. The job
        // consumer updates current_tier on success.
        sqlx::query(
            "UPDATE warehouse_partitions SET last_tier_evaluation_at = NOW() WHERE id = $1",
        )
        .bind(partition.id)
        .execute(&mut *tx)
        .await?;

        // Publish to Kafka before committing — if this fails, the transaction
        // is rolled back so no stale job or tier update remains in the DB.
        let kafka_msg = SyncJobKafkaMessage {
            job_id,
            job_type: job_type.to_string(),
            source_id: source.id,
            project_id: source.project_id,
            table_name: Some(partition.table_name.clone()),
            created_at: Utc::now().to_rfc3339(),
        };

        if let Err(e) = sender.send_sync_job(&kafka_msg).await {
            error!(
                job_id = %job_id,
                source_id = %source.id,
                error = %e,
                "Failed to publish tier transition job to Kafka, rolling back"
            );
            // Roll back the transaction — the job row and tier update are discarded
            tx.rollback().await?;
            continue;
        }

        // Kafka publish succeeded — commit the transaction
        tx.commit().await?;
        transition_count += 1;
    }

    Ok(transition_count)
}

/// Determine the job type and intermediate tier for a tier transition.
///
/// Multi-step transitions (Hot->Cold, Cold->Hot) go through an intermediate
/// Warm step. Returns `None` for same-tier or unexpected combinations.
fn determine_job_type(current: &StorageTier, target: &StorageTier) -> Option<(JobType, StorageTier)> {
    match (current, target) {
        (StorageTier::Hot, StorageTier::Warm) => Some((JobType::DowngradeToWarm, StorageTier::Warm)),
        (StorageTier::Warm, StorageTier::Cold) => Some((JobType::DowngradeToCold, StorageTier::Cold)),
        (StorageTier::Cold, StorageTier::Warm) => Some((JobType::UpgradeToWarm, StorageTier::Warm)),
        (StorageTier::Warm, StorageTier::Hot) => Some((JobType::UpgradeToHot, StorageTier::Hot)),
        (StorageTier::Hot, StorageTier::Cold) => {
            // Two-step: hot -> warm -> cold. Publish hot -> warm first.
            Some((JobType::DowngradeToWarm, StorageTier::Warm))
        }
        (StorageTier::Cold, StorageTier::Hot) => {
            // Two-step: cold -> warm -> hot. Publish cold -> warm first.
            Some((JobType::UpgradeToWarm, StorageTier::Warm))
        }
        _ => None,
    }
}

/// Determine the target storage tier for a partition given its age.
///
/// Walks the transitions sorted by `after_days` descending. The first rule
/// where `age_days >= after_days` determines the target tier.
fn determine_target_tier(transitions: &[TierTransition], age_days: u32) -> Option<StorageTier> {
    // Sort transitions by after_days descending so the longest-lived rule matches first
    let mut sorted: Vec<&TierTransition> = transitions.iter().collect();
    sorted.sort_by(|a, b| b.after_days.cmp(&a.after_days));

    for transition in sorted {
        if age_days >= transition.after_days {
            return Some(transition.tier);
        }
    }

    None
}

/// Find all committed partitions for a source.
#[tracing::instrument(
    name = "warehouse.lifecycle.find_source_partitions",
    skip_all,
    err(Display),
)]
async fn find_source_partitions(
    db: &PgPool,
    source_id: Uuid,
) -> Result<Vec<PartitionInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = sqlx::query(
        r#"
        SELECT id, table_name, partition_date, current_tier
        FROM warehouse_partitions
        WHERE source_id = $1
          AND sync_state = 'committed'
        "#,
    )
    .bind(source_id)
    .fetch_all(db)
    .await?;

    let mut partitions = Vec::with_capacity(rows.len());

    for row in rows {
        let id: Uuid = row.get("id");
        let table_name: String = row.get("table_name");
        let partition_date: chrono::NaiveDate = row.get("partition_date");
        let current_tier_str: String = row.get("current_tier");

        let current_tier = crate::warehouse::sources::types::parse_storage_tier(&current_tier_str);

        partitions.push(PartitionInfo {
            id,
            table_name,
            partition_date,
            current_tier,
        });
    }

    Ok(partitions)
}


/// Update only the last_tier_evaluation_at timestamp (tier unchanged).
#[tracing::instrument(
    name = "warehouse.lifecycle.update_tier_evaluation",
    skip_all,
    err(Display),
)]
async fn update_tier_evaluation(
    db: &PgPool,
    partition_id: Uuid,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sqlx::query(
        "UPDATE warehouse_partitions
         SET last_tier_evaluation_at = NOW()
         WHERE id = $1",
    )
    .bind(partition_id)
    .execute(db)
    .await?;

    Ok(())
}

// ============================================================================
// Access-Based Tier Policy Evaluation
// ============================================================================

/// A source with an access-based tier policy.
struct AccessBasedSource {
    id: Uuid,
    project_id: Uuid,
    name: String,
    current_tier: StorageTier,
    sensitivity: AccessSensitivity,
    supports_cdc: bool,
}

/// Find all enabled sources with access-based tier policies.
#[tracing::instrument(
    name = "warehouse.lifecycle.find_access_based_sources",
    skip_all,
    err(Display),
)]
async fn find_access_based_sources(
    db: &PgPool,
) -> Result<Vec<AccessBasedSource>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, name, tier, storage_tier_policy, supports_cdc
        FROM warehouse_sources
        WHERE storage_tier_policy->>'type' = 'access_based'
          AND enabled = true
        "#,
    )
    .fetch_all(db)
    .await?;

    let mut sources = Vec::with_capacity(rows.len());

    for row in rows {
        let id: Uuid = row.get("id");
        let project_id: Uuid = row.get("project_id");
        let name: String = row.get("name");
        let tier_str: String = row.get("tier");
        let policy_json: serde_json::Value = row.get("storage_tier_policy");
        let supports_cdc: bool = row.get("supports_cdc");

        let current_tier = crate::warehouse::sources::types::parse_storage_tier(&tier_str);

        // Parse the sensitivity from the policy JSON
        let policy: StorageTierPolicy = match serde_json::from_value(policy_json.clone()) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    source_id = %id,
                    error = %e,
                    policy = %policy_json,
                    "Failed to parse access-based tier policy, skipping source"
                );
                continue;
            }
        };

        if let StorageTierPolicy::AccessBased { sensitivity } = policy {
            sources.push(AccessBasedSource {
                id,
                project_id,
                name,
                current_tier,
                sensitivity,
                supports_cdc,
            });
        }
    }

    Ok(sources)
}

/// Evaluate an access-based source: count queries in the evaluation window
/// and promote or demote the source one tier if thresholds are crossed.
///
/// Returns `true` if a tier transition was published.
#[tracing::instrument(
    name = "pond.lifecycle.evaluate_access_based_source",
    skip(db, sender, source),
    fields(source_id = %source.id),
)]
async fn evaluate_access_based_source<S: SyncJobSender>(
    db: &PgPool,
    sender: &S,
    source: &AccessBasedSource,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let thresholds = source.sensitivity.thresholds();

    // Count queries for this source in the evaluation window
    let query_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM source_access_log
         WHERE source_id = $1
           AND accessed_at > NOW() - make_interval(days => $2)",
    )
    .bind(source.id)
    .bind(thresholds.window_days as i32)
    .fetch_one(db)
    .await?;

    let query_count = query_count as u64;

    // Determine action: promote, demote, or no change
    let action = if query_count > thresholds.promote_above {
        // Frequently accessed — promote to hotter tier
        if source.current_tier.is_hot() {
            None // Already at the hottest tier
        } else if !source.supports_cdc {
            // Non-CDC sources cannot be upgraded beyond cold
            info!(
                source_id = %source.id,
                source_name = %source.name,
                query_count = query_count,
                "Source qualifies for promotion but does not support CDC, skipping"
            );
            None
        } else {
            let target = match source.current_tier {
                StorageTier::Cold => StorageTier::Warm,
                StorageTier::Warm => StorageTier::Hot,
                StorageTier::Hot => unreachable!(),
            };
            Some(("promote", target))
        }
    } else if query_count < thresholds.demote_below {
        // Infrequently accessed — demote to colder tier
        if source.current_tier.is_cold() {
            None // Already at the coldest tier
        } else {
            let target = match source.current_tier {
                StorageTier::Hot => StorageTier::Warm,
                StorageTier::Warm => StorageTier::Cold,
                StorageTier::Cold => unreachable!(),
            };
            Some(("demote", target))
        }
    } else {
        None // Within normal range
    };

    let Some((direction, target_tier)) = action else {
        return Ok(false);
    };

    // Determine the job type for this transition
    let Some((job_type, _actual_next_tier)) =
        determine_job_type(&source.current_tier, &target_tier)
    else {
        return Ok(false);
    };

    info!(
        source_id = %source.id,
        source_name = %source.name,
        current_tier = %source.current_tier,
        target_tier = %target_tier,
        query_count = query_count,
        direction = direction,
        window_days = thresholds.window_days,
        "Access-based tier transition needed for source"
    );

    // Use a transaction for the DB operations
    let mut tx = db.begin().await?;

    // Atomic dedup+insert to avoid TOCTOU race
    let job_id = Uuid::new_v4();
    let insert_result = sqlx::query(
        "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at)
         SELECT $1, $2, $3, 'pending', NOW()
         WHERE NOT EXISTS (
             SELECT 1 FROM warehouse_jobs
             WHERE source_id = $3
               AND job_type = $2
               AND status IN ('pending', 'running')
         )",
    )
    .bind(job_id)
    .bind(job_type.to_string())
    .bind(source.id)
    .execute(&mut *tx)
    .await?;

    if insert_result.rows_affected() == 0 {
        tx.rollback().await?;
        info!(
            source_id = %source.id,
            job_type = %job_type,
            "Skipping access-based transition, active job already exists for source"
        );
        return Ok(false);
    }

    // Do NOT update the source tier here — the tier should only change once
    // the transition job completes successfully. The job consumer updates the
    // source tier on success. This prevents queries from routing to a tier
    // whose data hasn't been moved yet.

    // Publish to Kafka before committing
    let kafka_msg = SyncJobKafkaMessage {
        job_id,
        job_type: job_type.to_string(),
        source_id: source.id,
        project_id: source.project_id,
        table_name: None, // Source-level transition, not partition-level
        created_at: Utc::now().to_rfc3339(),
    };

    if let Err(e) = sender.send_sync_job(&kafka_msg).await {
        error!(
            job_id = %job_id,
            source_id = %source.id,
            error = %e,
            "Failed to publish access-based tier transition job to Kafka, rolling back"
        );
        tx.rollback().await?;
        return Ok(false);
    }

    tx.commit().await?;
    Ok(true)
}

/// Delete rows from `source_access_log` older than the maximum evaluation window.
///
/// The maximum window is 30 days (Conservative sensitivity). Rows older than that
/// are never needed and can be safely removed to prevent unbounded table growth.
#[tracing::instrument(
    name = "warehouse.lifecycle.cleanup_old_access_logs",
    skip_all,
    err(Display),
)]
async fn cleanup_old_access_logs(
    db: &PgPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let max_days = AccessSensitivity::max_window_days() as i32;

    let result = sqlx::query(
        "DELETE FROM source_access_log WHERE accessed_at < NOW() - make_interval(days => $1)",
    )
    .bind(max_days)
    .execute(db)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            deleted_rows = deleted,
            max_age_days = max_days,
            "Cleaned up old source_access_log rows"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockSyncJobSender {
        messages: Arc<Mutex<Vec<SyncJobKafkaMessage>>>,
    }

    impl MockSyncJobSender {
        fn new() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn sent_messages(&self) -> Vec<SyncJobKafkaMessage> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl SyncJobSender for MockSyncJobSender {
        async fn send_sync_job(&self, message: &SyncJobKafkaMessage) -> anyhow::Result<()> {
            self.messages.lock().unwrap().push(message.clone());
            Ok(())
        }
    }

    async fn create_test_pool() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for integration tests");
        PgPool::connect(&url).await.expect("Failed to connect to test database")
    }

    async fn insert_test_source(
        db: &PgPool,
        source_id: Uuid,
        project_id: Uuid,
        name: &str,
        tier: &str,
        policy: &serde_json::Value,
        supports_cdc: bool,
    ) {
        sqlx::query(
            r#"INSERT INTO warehouse_sources
               (id, project_id, name, source_type, storage_type, config, tier,
                storage_tier_policy, supports_cdc, connection_config_hash)
               VALUES ($1, $2, $3, 'postgresql', 'object_storage', '{}', $4, $5, $6, $7)"#,
        )
        .bind(source_id)
        .bind(project_id)
        .bind(name)
        .bind(tier)
        .bind(policy)
        .bind(supports_cdc)
        .bind(Uuid::new_v4().to_string())
        .execute(db)
        .await
        .expect("Failed to insert test source");
    }

    async fn insert_test_partition(
        db: &PgPool,
        source_id: Uuid,
        table_name: &str,
        partition_date: chrono::NaiveDate,
        current_tier: &str,
    ) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO warehouse_partitions
               (id, source_id, table_name, partition_date, current_tier, sync_state)
               VALUES ($1, $2, $3, $4, $5, 'committed')"#,
        )
        .bind(id)
        .bind(source_id)
        .bind(table_name)
        .bind(partition_date)
        .bind(current_tier)
        .execute(db)
        .await
        .expect("Failed to insert test partition");
        id
    }

    async fn cleanup_test_data(db: &PgPool, source_id: Uuid) {
        let _ = sqlx::query("DELETE FROM warehouse_jobs WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM source_access_log WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM warehouse_partitions WHERE source_id = $1")
            .bind(source_id)
            .execute(db)
            .await;
        let _ = sqlx::query("DELETE FROM warehouse_sources WHERE id = $1")
            .bind(source_id)
            .execute(db)
            .await;
    }

    // ========================================================================
    // Integration tests (require Postgres)
    // ========================================================================

    #[tokio::test]
    #[ignore = "requires Postgres on DATABASE_URL"]
    async fn test_lifecycle_age_based_transitions() {
        let db = create_test_pool().await;
        let sender = MockSyncJobSender::new();

        let source_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let policy = serde_json::json!({
            "type": "lifecycle",
            "transitions": [
                { "after_days": 0, "tier": "hot" },
                { "after_days": 30, "tier": "warm" },
                { "after_days": 90, "tier": "cold" }
            ]
        });

        insert_test_source(&db, source_id, project_id, "test-lifecycle", "hot", &policy, true).await;

        let today = Utc::now().date_naive();

        // 5 days old, currently hot -> should stay hot (target = hot)
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(5), "hot").await;
        // 45 days old, currently hot -> target = warm, needs DowngradeToWarm
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(45), "hot").await;
        // 100 days old, currently hot -> target = cold, two-step: first DowngradeToWarm
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(100), "hot").await;

        let transitions = evaluate_tiers(&db, &sender).await.unwrap();

        let msgs = sender.sent_messages();
        let our_msgs: Vec<_> = msgs.iter().filter(|m| m.source_id == source_id).collect();

        assert_eq!(our_msgs.len(), 2, "Expected 2 transitions, got {}", our_msgs.len());
        assert!(our_msgs.iter().all(|m| m.project_id == project_id));

        let job_types: Vec<&str> = our_msgs.iter().map(|m| m.job_type.as_str()).collect();
        assert_eq!(
            job_types.iter().filter(|&&jt| jt == "downgrade_to_warm").count(),
            2,
            "Both transitions should be downgrade_to_warm (45-day direct, 100-day two-step)"
        );

        assert!(transitions >= 2);

        let pending_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM warehouse_jobs WHERE source_id = $1 AND status = 'pending'"
        )
        .bind(source_id)
        .fetch_one(&db)
        .await
        .unwrap();
        assert!(pending_jobs >= 1, "At least one pending job should exist in DB");

        cleanup_test_data(&db, source_id).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres on DATABASE_URL"]
    async fn test_access_based_promotion() {
        let db = create_test_pool().await;
        let sender = MockSyncJobSender::new();

        let source_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let policy = serde_json::json!({
            "type": "access_based",
            "sensitivity": "aggressive"
        });

        // Aggressive: window_days=7, promote_above=20
        insert_test_source(&db, source_id, project_id, "test-access-promo", "warm", &policy, true).await;

        // Insert 25 access log entries (above promote threshold of 20)
        for _ in 0..25 {
            sqlx::query(
                "INSERT INTO source_access_log (source_id, project_id, accessed_at) VALUES ($1, $2, NOW())"
            )
            .bind(source_id)
            .bind(project_id)
            .execute(&db)
            .await
            .unwrap();
        }

        evaluate_tiers(&db, &sender).await.unwrap();

        let msgs = sender.sent_messages();
        let our_msgs: Vec<_> = msgs.iter().filter(|m| m.source_id == source_id).collect();

        assert_eq!(our_msgs.len(), 1, "Expected 1 promotion, got {}", our_msgs.len());
        assert_eq!(our_msgs[0].job_type, "upgrade_to_hot");

        cleanup_test_data(&db, source_id).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres on DATABASE_URL"]
    async fn test_access_based_demotion() {
        let db = create_test_pool().await;
        let sender = MockSyncJobSender::new();

        let source_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let policy = serde_json::json!({
            "type": "access_based",
            "sensitivity": "aggressive"
        });

        // Aggressive: window_days=7, demote_below=5. Zero access logs -> demotion.
        insert_test_source(&db, source_id, project_id, "test-access-demote", "hot", &policy, true).await;

        evaluate_tiers(&db, &sender).await.unwrap();

        let msgs = sender.sent_messages();
        let our_msgs: Vec<_> = msgs.iter().filter(|m| m.source_id == source_id).collect();

        assert_eq!(our_msgs.len(), 1, "Expected 1 demotion, got {}", our_msgs.len());
        assert_eq!(our_msgs[0].job_type, "downgrade_to_warm");

        cleanup_test_data(&db, source_id).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres on DATABASE_URL"]
    async fn test_lifecycle_noop_when_tier_matches() {
        let db = create_test_pool().await;
        let sender = MockSyncJobSender::new();

        let source_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let policy = serde_json::json!({
            "type": "lifecycle",
            "transitions": [
                { "after_days": 0, "tier": "hot" },
                { "after_days": 30, "tier": "warm" }
            ]
        });

        insert_test_source(&db, source_id, project_id, "test-noop", "hot", &policy, true).await;

        let today = Utc::now().date_naive();

        // 5 days old, currently hot -> target = hot, no transition needed
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(5), "hot").await;
        // 45 days old, currently warm -> target = warm, no transition needed
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(45), "warm").await;

        evaluate_tiers(&db, &sender).await.unwrap();

        let msgs = sender.sent_messages();
        let our_msgs: Vec<_> = msgs.iter().filter(|m| m.source_id == source_id).collect();
        assert_eq!(our_msgs.len(), 0, "Expected 0 transitions when tiers already match");

        cleanup_test_data(&db, source_id).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres on DATABASE_URL"]
    async fn test_lifecycle_dedup_prevents_duplicate_jobs() {
        let db = create_test_pool().await;
        let sender = MockSyncJobSender::new();

        let source_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        let policy = serde_json::json!({
            "type": "lifecycle",
            "transitions": [
                { "after_days": 0, "tier": "hot" },
                { "after_days": 30, "tier": "warm" }
            ]
        });

        insert_test_source(&db, source_id, project_id, "test-dedup", "hot", &policy, true).await;

        let today = Utc::now().date_naive();
        // 45 days old, currently hot -> target = warm, needs transition
        insert_test_partition(&db, source_id, "events", today - chrono::Days::new(45), "hot").await;

        // Pre-insert a pending job for the same source/job_type
        sqlx::query(
            "INSERT INTO warehouse_jobs (id, job_type, source_id, status, scheduled_at) VALUES ($1, $2, $3, 'pending', NOW())"
        )
        .bind(Uuid::new_v4())
        .bind("downgrade_to_warm")
        .bind(source_id)
        .execute(&db)
        .await
        .unwrap();

        evaluate_tiers(&db, &sender).await.unwrap();

        let msgs = sender.sent_messages();
        let our_msgs: Vec<_> = msgs.iter().filter(|m| m.source_id == source_id).collect();
        assert_eq!(our_msgs.len(), 0, "Dedup should prevent sending when pending job exists");

        cleanup_test_data(&db, source_id).await;
    }

    // ========================================================================
    // Unit tests (no DB required)
    // ========================================================================

    #[test]
    fn test_lifecycle_worker_config_default() {
        let config = LifecycleWorkerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3600));
    }

    #[test]
    fn test_determine_target_tier_basic() {
        let transitions = vec![
            TierTransition {
                after_days: 0,
                tier: StorageTier::Hot,
            },
            TierTransition {
                after_days: 30,
                tier: StorageTier::Warm,
            },
            TierTransition {
                after_days: 90,
                tier: StorageTier::Cold,
            },
        ];

        // Brand new data (0 days old) -> Hot
        assert_eq!(
            determine_target_tier(&transitions, 0),
            Some(StorageTier::Hot)
        );

        // 15 days old -> Hot (only 0-day rule matches)
        assert_eq!(
            determine_target_tier(&transitions, 15),
            Some(StorageTier::Hot)
        );

        // 30 days old -> Warm
        assert_eq!(
            determine_target_tier(&transitions, 30),
            Some(StorageTier::Warm)
        );

        // 60 days old -> Warm (30-day rule is highest match)
        assert_eq!(
            determine_target_tier(&transitions, 60),
            Some(StorageTier::Warm)
        );

        // 90 days old -> Cold
        assert_eq!(
            determine_target_tier(&transitions, 90),
            Some(StorageTier::Cold)
        );

        // 365 days old -> Cold
        assert_eq!(
            determine_target_tier(&transitions, 365),
            Some(StorageTier::Cold)
        );
    }

    #[test]
    fn test_determine_target_tier_no_transitions() {
        let transitions: Vec<TierTransition> = vec![];
        assert_eq!(determine_target_tier(&transitions, 30), None);
    }

    #[test]
    fn test_determine_target_tier_single_transition() {
        let transitions = vec![TierTransition {
            after_days: 7,
            tier: StorageTier::Cold,
        }];

        // 3 days old -> None (below threshold)
        assert_eq!(determine_target_tier(&transitions, 3), None);

        // 7 days old -> Cold
        assert_eq!(
            determine_target_tier(&transitions, 7),
            Some(StorageTier::Cold)
        );

        // 100 days old -> Cold
        assert_eq!(
            determine_target_tier(&transitions, 100),
            Some(StorageTier::Cold)
        );
    }

    #[test]
    fn test_determine_target_tier_unsorted_input() {
        // Transitions provided in random order — should still work
        let transitions = vec![
            TierTransition {
                after_days: 90,
                tier: StorageTier::Cold,
            },
            TierTransition {
                after_days: 0,
                tier: StorageTier::Hot,
            },
            TierTransition {
                after_days: 30,
                tier: StorageTier::Warm,
            },
        ];

        assert_eq!(
            determine_target_tier(&transitions, 5),
            Some(StorageTier::Hot)
        );
        assert_eq!(
            determine_target_tier(&transitions, 45),
            Some(StorageTier::Warm)
        );
        assert_eq!(
            determine_target_tier(&transitions, 100),
            Some(StorageTier::Cold)
        );
    }

    #[test]
    fn test_determine_target_tier_exact_boundary() {
        let transitions = vec![
            TierTransition { after_days: 30, tier: StorageTier::Warm },
        ];
        // Exactly at boundary -> matches
        assert_eq!(determine_target_tier(&transitions, 30), Some(StorageTier::Warm));
        // One day before -> no match
        assert_eq!(determine_target_tier(&transitions, 29), None);
    }

    #[test]
    fn test_determine_target_tier_zero_days_no_zero_rule() {
        // If there's no 0-day rule, a 0-day-old partition should match nothing
        let transitions = vec![
            TierTransition { after_days: 30, tier: StorageTier::Warm },
            TierTransition { after_days: 90, tier: StorageTier::Cold },
        ];
        assert_eq!(determine_target_tier(&transitions, 0), None);
    }

    #[test]
    fn test_determine_target_tier_multiple_transitions_boundaries() {
        let transitions = vec![
            TierTransition { after_days: 0, tier: StorageTier::Hot },
            TierTransition { after_days: 30, tier: StorageTier::Warm },
            TierTransition { after_days: 90, tier: StorageTier::Cold },
        ];

        // Just below each transition boundary
        assert_eq!(determine_target_tier(&transitions, 29), Some(StorageTier::Hot));
        assert_eq!(determine_target_tier(&transitions, 89), Some(StorageTier::Warm));

        // Exactly at each transition boundary
        assert_eq!(determine_target_tier(&transitions, 30), Some(StorageTier::Warm));
        assert_eq!(determine_target_tier(&transitions, 90), Some(StorageTier::Cold));
    }

    #[test]
    fn test_determine_job_type_direct_transitions() {
        // Direct single-step transitions
        assert_eq!(
            determine_job_type(&StorageTier::Hot, &StorageTier::Warm),
            Some((JobType::DowngradeToWarm, StorageTier::Warm))
        );
        assert_eq!(
            determine_job_type(&StorageTier::Warm, &StorageTier::Cold),
            Some((JobType::DowngradeToCold, StorageTier::Cold))
        );
        assert_eq!(
            determine_job_type(&StorageTier::Cold, &StorageTier::Warm),
            Some((JobType::UpgradeToWarm, StorageTier::Warm))
        );
        assert_eq!(
            determine_job_type(&StorageTier::Warm, &StorageTier::Hot),
            Some((JobType::UpgradeToHot, StorageTier::Hot))
        );
    }

    #[test]
    fn test_determine_job_type_two_step_transitions() {
        // Hot -> Cold goes through Warm first
        assert_eq!(
            determine_job_type(&StorageTier::Hot, &StorageTier::Cold),
            Some((JobType::DowngradeToWarm, StorageTier::Warm))
        );

        // Cold -> Hot goes through Warm first
        assert_eq!(
            determine_job_type(&StorageTier::Cold, &StorageTier::Hot),
            Some((JobType::UpgradeToWarm, StorageTier::Warm))
        );
    }

    #[test]
    fn test_determine_job_type_same_tier() {
        // Same tier returns None
        assert_eq!(determine_job_type(&StorageTier::Hot, &StorageTier::Hot), None);
        assert_eq!(determine_job_type(&StorageTier::Warm, &StorageTier::Warm), None);
        assert_eq!(determine_job_type(&StorageTier::Cold, &StorageTier::Cold), None);
    }
}
