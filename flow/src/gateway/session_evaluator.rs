//! Background producer that discovers idle LLM sessions and enqueues them
//! to Kafka for evaluation. This is the producer half of the session
//! evaluation pipeline; the consumer half lives in `session_eval_consumer`.
//!
//! Runs on a 60-second interval. A session is considered "idle" when its
//! most recent request is older than 30 minutes. Only sessions within a
//! 24-hour lookback window are discovered (no backfilling historical sessions).
//!
//! Redis SET-based deduplication prevents re-enqueuing sessions that are
//! already in-flight for evaluation. The same dedup logic is shared with
//! the explicit "end session" endpoint in `routes/mod.rs`.

use std::sync::Arc;

use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::kafka::{KafkaProducer, SessionEvalJobKafkaMessage};

use crate::app_state::RedisPool;

const IDLE_TIMEOUT_MINUTES: u32 = 30;
const LOOKBACK_HOURS: u32 = 24;
const EVAL_INTERVAL_SECS: u64 = 60;
const BATCH_LIMIT: u32 = 1000;
const REDIS_DEDUP_KEY: &str = "session_eval:enqueued";
const REDIS_DEDUP_TTL_SECS: i64 = 7200; // 2 hours

/// Result of an enqueue attempt via [`enqueue_session_eval`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueResult {
    /// Session was newly enqueued for evaluation.
    Enqueued,
    /// Session was already in the dedup set (no-op).
    AlreadyEnqueued,
    /// Kafka send failed; dedup entry was rolled back.
    Failed,
}

/// Enqueue a single session for evaluation via Kafka, with Redis dedup.
///
/// Returns [`EnqueueResult::Enqueued`] on success,
/// [`EnqueueResult::AlreadyEnqueued`] if the session is already in-flight,
/// or [`EnqueueResult::Failed`] if the Kafka send fails (dedup rolled back).
///
/// Shared by the idle-poll producer and the explicit `/end` endpoint.
pub async fn enqueue_session_eval(
    kafka: &KafkaProducer,
    redis: &RedisPool,
    project_id: &str,
    session_id: &str,
) -> EnqueueResult {
    let dedup_member = format!("{project_id}:{session_id}");

    if !try_mark_enqueued(redis, &dedup_member).await {
        return EnqueueResult::AlreadyEnqueued;
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
        undo_mark_enqueued(redis, &dedup_member).await;
        return EnqueueResult::Failed;
    }

    EnqueueResult::Enqueued
}

/// Try to mark a session as enqueued without sending to Kafka.
/// Used by the `/end` endpoint to reserve the dedup slot before
/// spawning the delayed Kafka send.
pub async fn try_reserve_session(redis: &RedisPool, project_id: &str, session_id: &str) -> bool {
    let dedup_member = format!("{project_id}:{session_id}");
    try_mark_enqueued(redis, &dedup_member).await
}

/// Roll back a dedup reservation. Used when the delayed Kafka send fails.
pub async fn unreserve_session(redis: &RedisPool, project_id: &str, session_id: &str) {
    let dedup_member = format!("{project_id}:{session_id}");
    undo_mark_enqueued(redis, &dedup_member).await;
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

/// Remove a member from the dedup set -- used to roll back when the
/// Kafka send fails after a successful `try_mark_enqueued`.
async fn undo_mark_enqueued(redis: &RedisPool, member: &str) {
    let Ok(mut conn) = redis.get().await else {
        return;
    };
    let _ = redis::cmd("SREM")
        .arg(REDIS_DEDUP_KEY)
        .arg(member)
        .query_async::<i64>(&mut *conn)
        .await;
}

/// Atomically add a session to the Redis dedup set. Returns `true` if the
/// member was newly added (i.e. not already present), `false` if it was
/// already in the set or on Redis error. Also refreshes the set TTL on
/// successful addition.
async fn try_mark_enqueued(redis: &RedisPool, member: &str) -> bool {
    let Ok(mut conn) = redis.get().await else {
        return false;
    };
    let added: i64 = redis::cmd("SADD")
        .arg(REDIS_DEDUP_KEY)
        .arg(member)
        .query_async(&mut *conn)
        .await
        .unwrap_or(0);
    if added == 1 {
        let _ = redis::cmd("EXPIRE")
            .arg(REDIS_DEDUP_KEY)
            .arg(REDIS_DEDUP_TTL_SECS)
            .query_async::<i64>(&mut *conn)
            .await;
    }
    added == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_result_equality() {
        assert_eq!(EnqueueResult::Enqueued, EnqueueResult::Enqueued);
        assert_eq!(EnqueueResult::AlreadyEnqueued, EnqueueResult::AlreadyEnqueued);
        assert_eq!(EnqueueResult::Failed, EnqueueResult::Failed);
        assert_ne!(EnqueueResult::Enqueued, EnqueueResult::AlreadyEnqueued);
        assert_ne!(EnqueueResult::Enqueued, EnqueueResult::Failed);
    }

    #[test]
    fn dedup_member_format() {
        let project_id = "proj-123";
        let session_id = "sess-456";
        let member = format!("{project_id}:{session_id}");
        assert_eq!(member, "proj-123:sess-456");
    }
}
