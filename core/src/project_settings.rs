//! Cached per-project settings lookups.
//!
//! Follows the same Redis caching pattern as `pii.rs` to avoid
//! per-span database queries in hot ingestion paths.

use bb8_redis::redis::AsyncCommands;
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::db::DbPool;

const SETTING_CACHE_TTL_SECS: u64 = 300;

pub async fn get_span_metrics_enabled(db: &DbPool, project_id: Uuid) -> bool {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT COALESCE((settings->>'span_metrics_enabled')::boolean, false) FROM projects WHERE id = $1",
    )
    .bind(project_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.map(|r| r.0).unwrap_or(false)
}

pub async fn get_span_metrics_enabled_cached(
    redis: &RedisPool,
    db: &DbPool,
    project_id: Uuid,
) -> bool {
    let cache_key = format!("span_metrics_enabled:{}", project_id);

    if let Ok(mut conn) = redis.get().await {
        let cached: Option<String> =
            tokio::time::timeout(Duration::from_secs(1), conn.get(&cache_key))
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten();

        if let Some(value) = cached {
            debug!("span_metrics_enabled cache hit for project_id={}", project_id);
            return value == "1";
        }
    }

    debug!(
        "span_metrics_enabled cache miss for project_id={}, querying database",
        project_id
    );
    let enabled = get_span_metrics_enabled(db, project_id).await;

    if let Ok(mut conn) = redis.get().await {
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            conn.set_ex::<_, _, ()>(
                &cache_key,
                if enabled { "1" } else { "0" },
                SETTING_CACHE_TTL_SECS,
            ),
        )
        .await;
    }

    enabled
}

pub async fn invalidate_span_metrics_cache(redis: &RedisPool, project_id: Uuid) {
    let cache_key = format!("span_metrics_enabled:{}", project_id);

    if let Ok(mut conn) = redis.get().await {
        let _ = conn.del::<_, ()>(&cache_key).await;
        debug!(
            "Invalidated span_metrics_enabled cache for project_id={}",
            project_id
        );
    }
}
