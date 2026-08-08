//! LLM Rollout Cache Utilities
//!
//! Shared cache invalidation functions for rollout-related Redis caching.

use bb8_redis::redis::AsyncCommands;
use uuid::Uuid;

use crate::app_state::RedisPool;

/// Invalidate rollout cache for a project and config.
///
/// Called when rollout state changes (start, pause, promote, rollback, complete).
/// This ensures the gateway picks up the latest rollout configuration.
///
/// Deletes both project-level and config-level cache keys:
/// - `rollout:project:{project_id}` - For finding active rollout by project
/// - `rollout:config:{config_id}` - For finding active rollout by config
pub async fn invalidate_rollout_cache(redis: &RedisPool, project_id: Uuid, config_id: Uuid) {
    let project_key = format!("rollout:project:{}", project_id);
    let config_key = format!("rollout:config:{}", config_id);

    match redis.get().await {
        Ok(mut conn) => {
            if let Err(e) = conn.del::<_, ()>(&[&project_key, &config_key]).await {
                tracing::warn!(
                    project_id = %project_id,
                    config_id = %config_id,
                    error = %e,
                    "Failed to invalidate rollout cache"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                project_id = %project_id,
                config_id = %config_id,
                error = %e,
                "Failed to get Redis connection for cache invalidation"
            );
        }
    }
}

/// Invalidate cached prompt version configs.
///
/// Removes `prompt_version:{version_id}` keys so the gateway does not serve
/// stale version data after a config (and its CASCADE-deleted versions) is removed.
pub async fn invalidate_prompt_version_cache(redis: &RedisPool, version_ids: &[Uuid]) {
    if version_ids.is_empty() {
        return;
    }

    let keys: Vec<String> = version_ids
        .iter()
        .map(|id| format!("prompt_version:{}", id))
        .collect();
    let key_refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();

    match redis.get().await {
        Ok(mut conn) => {
            if let Err(e) = conn.del::<_, ()>(key_refs.as_slice()).await {
                tracing::warn!(
                    count = version_ids.len(),
                    error = %e,
                    "Failed to invalidate prompt version cache"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                count = version_ids.len(),
                error = %e,
                "Failed to get Redis connection for version cache invalidation"
            );
        }
    }
}
