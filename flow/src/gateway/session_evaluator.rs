//! Background producer that discovers idle LLM sessions and enqueues them
//! to Kafka for evaluation. This is the producer half of the session
//! evaluation pipeline; the consumer half lives in `session_eval_consumer`.
//!
//! Runs on a 60-second interval. A session is considered "idle" when its
//! most recent request is older than 30 minutes. Only sessions within a
//! 24-hour lookback window are discovered (no backfilling historical sessions).
//!
//! Redis per-session keys prevent re-enqueuing sessions that are already
//! in-flight for evaluation. The same dedup logic is shared with the explicit
//! "end session" endpoint in `routes/mod.rs`. Per-session expiry is important:
//! a busy project must not keep a crashed reservation alive indefinitely by
//! refreshing one shared set's TTL.

use std::sync::Arc;

use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::kafka::{KafkaProducer, SessionEvalJobKafkaMessage};

use crate::app_state::RedisPool;

const IDLE_TIMEOUT_MINUTES: u32 = 30;
const LOOKBACK_HOURS: u32 = 24;
const EVAL_INTERVAL_SECS: u64 = 60;
const BATCH_LIMIT: u32 = 1000;
const REDIS_DEDUP_KEY_PREFIX: &str = "session_eval:enqueued";
const REDIS_DEDUP_TTL_SECS: i64 = 7200; // 2 hours
const REDIS_RESERVATION_TTL_SECS: i64 = 300; // 5 minutes

/// Delay used by the explicit `/end` route before it sends the evaluation job.
/// The short reservation TTL above must remain longer than this delay but
/// shorter than idle discovery so a restarted task can be recovered.
pub const END_SESSION_DELAY_SECS: u64 = 30;

/// Result of an enqueue attempt via [`enqueue_session_eval`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueResult {
    /// Session was newly enqueued for evaluation.
    Enqueued,
    /// Session already has a dedup key (no-op).
    AlreadyEnqueued,
    /// Redis was unavailable, or Kafka send failed and the dedup entry was rolled back.
    Failed,
}

/// Result of atomically reserving a session's evaluation slot in Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationResult {
    /// The reservation was created by this request.
    Reserved,
    /// Another request already reserved or enqueued this session.
    AlreadyReserved,
    /// Redis could not confirm either outcome.
    Unavailable,
}

/// Enqueue a single session for evaluation via Kafka, with Redis dedup.
///
/// Returns [`EnqueueResult::Enqueued`] on success,
/// [`EnqueueResult::AlreadyEnqueued`] if the session is already in-flight,
/// or [`EnqueueResult::Failed`] if Redis is unavailable or the Kafka send
/// fails (dedup rolled back).
///
/// Shared by the idle-poll producer and the explicit `/end` endpoint.
pub async fn enqueue_session_eval(
    kafka: &KafkaProducer,
    redis: &RedisPool,
    project_id: &str,
    session_id: &str,
) -> EnqueueResult {
    let dedup_key = dedup_key(project_id, session_id);

    match try_mark_enqueued(redis, &dedup_key, REDIS_DEDUP_TTL_SECS).await {
        ReservationResult::Reserved => {}
        ReservationResult::AlreadyReserved => return EnqueueResult::AlreadyEnqueued,
        ReservationResult::Unavailable => return EnqueueResult::Failed,
    }

    let message = SessionEvalJobKafkaMessage {
        project_id: project_id.to_string(),
        session_id: session_id.to_string(),
        enqueued_at: chrono::Utc::now().to_rfc3339(),
    };

    if let Err(e) = kafka.send_session_eval_job(&message).await {
        tracing::warn!(
            %project_id, %session_id, error = %e,
            "Failed to enqueue session eval job"
        );
        undo_mark_enqueued(redis, &dedup_key).await;
        return EnqueueResult::Failed;
    }

    EnqueueResult::Enqueued
}

/// Try to mark a session as enqueued without sending to Kafka.
/// Used by the `/end` endpoint to reserve the dedup slot before
/// spawning the delayed Kafka send.
pub async fn try_reserve_session(
    redis: &RedisPool,
    project_id: &str,
    session_id: &str,
) -> ReservationResult {
    let dedup_key = dedup_key(project_id, session_id);
    try_mark_enqueued(redis, &dedup_key, REDIS_RESERVATION_TTL_SECS).await
}

/// Extend a successful explicit-end reservation to the normal deduplication
/// lifetime after its Kafka message has been accepted.
pub async fn confirm_session_enqueued(redis: &RedisPool, project_id: &str, session_id: &str) {
    let dedup_key = dedup_key(project_id, session_id);
    let Ok(mut conn) = redis.get().await else {
        tracing::warn!(%project_id, %session_id, "Failed to extend session eval dedup TTL");
        return;
    };
    let extended = redis::cmd("EXPIRE")
        .arg(&dedup_key)
        .arg(REDIS_DEDUP_TTL_SECS)
        .query_async::<i64>(&mut *conn)
        .await
        .unwrap_or(0);
    if extended != 1 {
        tracing::warn!(
            %project_id,
            %session_id,
            "Session eval reservation expired before confirmation"
        );
    }
}

/// Roll back a dedup reservation. Used when the delayed Kafka send fails.
pub async fn unreserve_session(redis: &RedisPool, project_id: &str, session_id: &str) {
    let dedup_key = dedup_key(project_id, session_id);
    undo_mark_enqueued(redis, &dedup_key).await;
}

/// Spawn the session evaluation producer as a background task.
pub fn spawn(
    clickhouse: Arc<ClickHousePool>,
    kafka: Arc<KafkaProducer>,
    redis: Arc<RedisPool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(EVAL_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = run_producer_cycle(&clickhouse, &kafka, &redis).await {
                        tracing::warn!(error = %e, "Session eval producer cycle failed");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        tracing::info!("Session eval producer shutting down");
    })
}

/// One producer cycle: find all idle sessions across all projects and enqueue to Kafka.
async fn run_producer_cycle(
    clickhouse: &ClickHousePool,
    kafka: &KafkaProducer,
    redis: &RedisPool,
) -> anyhow::Result<()> {
    let candidates = find_idle_sessions(clickhouse).await?;

    if candidates.is_empty() {
        return Ok(());
    }

    tracing::info!(
        candidate_count = candidates.len(),
        "Session eval producer: idle sessions found"
    );

    let mut enqueued = 0u64;
    for candidate in &candidates {
        if enqueue_session_eval(kafka, redis, &candidate.project_id, &candidate.session_id).await
            == EnqueueResult::Enqueued
        {
            enqueued += 1;
        }
    }

    if enqueued > 0 {
        tracing::info!(enqueued, "Session eval producer: jobs enqueued to Kafka");
    }

    Ok(())
}

struct IdleSession {
    project_id: String,
    session_id: String,
}

/// Find sessions idle for >30 min within the 24h lookback window, across all projects.
async fn find_idle_sessions(clickhouse: &ClickHousePool) -> anyhow::Result<Vec<IdleSession>> {
    let query = format!(
        r#"
        SELECT project_id, session_id
        FROM reiver.llm_requests
        WHERE session_id != ''
            AND timestamp > now() - INTERVAL {lookback} HOUR
        GROUP BY project_id, session_id
        HAVING max(timestamp) < now() - INTERVAL {idle} MINUTE
        ORDER BY max(timestamp) DESC
        LIMIT {limit}
        "#,
        lookback = LOOKBACK_HOURS,
        idle = IDLE_TIMEOUT_MINUTES,
        limit = BATCH_LIMIT,
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        project_id: String,
        session_id: String,
    }

    let rows: Vec<Row> = clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse idle sessions query error: {}", e))?;

    Ok(rows
        .into_iter()
        .map(|r| IdleSession {
            project_id: r.project_id,
            session_id: r.session_id,
        })
        .collect())
}

fn dedup_key(project_id: &str, session_id: &str) -> String {
    format!("{REDIS_DEDUP_KEY_PREFIX}:{project_id}:{session_id}")
}

/// Remove a per-session dedup key -- used to roll back when the Kafka send
/// fails after a successful `try_mark_enqueued`.
async fn undo_mark_enqueued(redis: &RedisPool, key: &str) {
    let Ok(mut conn) = redis.get().await else {
        return;
    };
    let _ = redis::cmd("DEL")
        .arg(key)
        .query_async::<i64>(&mut *conn)
        .await;
}

/// Atomically create a per-session dedup key. Distinguishes a genuine
/// duplicate from Redis failure so callers never report a false success.
/// Each session expires independently.
async fn try_mark_enqueued(redis: &RedisPool, key: &str, ttl_secs: i64) -> ReservationResult {
    let mut conn = match redis.get().await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error = %e, %key, "Failed to acquire Redis connection for session eval reservation");
            return ReservationResult::Unavailable;
        }
    };
    let result = redis::cmd("SET")
        .arg(key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(ttl_secs)
        .query_async::<Option<String>>(&mut *conn)
        .await;
    if let Err(e) = &result {
        tracing::error!(error = %e, %key, "Failed to reserve session evaluation in Redis");
    }
    classify_reservation_result(&result)
}

fn classify_reservation_result<T, E>(result: &Result<Option<T>, E>) -> ReservationResult {
    match result {
        Ok(Some(_)) => ReservationResult::Reserved,
        Ok(None) => ReservationResult::AlreadyReserved,
        Err(_) => ReservationResult::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_result_equality() {
        assert_eq!(EnqueueResult::Enqueued, EnqueueResult::Enqueued);
        assert_eq!(
            EnqueueResult::AlreadyEnqueued,
            EnqueueResult::AlreadyEnqueued
        );
        assert_eq!(EnqueueResult::Failed, EnqueueResult::Failed);
        assert_ne!(EnqueueResult::Enqueued, EnqueueResult::AlreadyEnqueued);
        assert_ne!(EnqueueResult::Enqueued, EnqueueResult::Failed);
    }

    #[test]
    fn redis_result_distinguishes_duplicate_from_unavailable() {
        assert_eq!(
            classify_reservation_result(&Ok::<Option<&str>, ()>(Some("OK"))),
            ReservationResult::Reserved
        );
        assert_eq!(
            classify_reservation_result(&Ok::<Option<&str>, ()>(None)),
            ReservationResult::AlreadyReserved
        );
        assert_eq!(
            classify_reservation_result(&Err::<Option<&str>, _>(())),
            ReservationResult::Unavailable,
        );
    }

    #[test]
    fn dedup_key_scopes_project_and_session() {
        let project_id = "proj-123";
        let session_id = "sess-456";
        assert_eq!(
            dedup_key(project_id, session_id),
            "session_eval:enqueued:proj-123:sess-456"
        );
    }

    #[test]
    fn explicit_end_reservation_expires_before_idle_discovery() {
        assert!(REDIS_RESERVATION_TTL_SECS > END_SESSION_DELAY_SECS as i64);
        assert!(REDIS_RESERVATION_TTL_SECS < IDLE_TIMEOUT_MINUTES as i64 * 60);
    }
}
