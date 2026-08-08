use dashmap::DashMap;
use sqlx::PgPool;
use uuid::Uuid;

/// In-memory cache of approved A2A access grants.
///
/// Keyed by `(granted_agent_id, target_agent_id)`. Presence in the cache
/// means the grant is approved. Warmed from the DB on startup and kept
/// in sync by approve/deny/revoke mutations.
pub struct AccessCache {
    inner: DashMap<(Uuid, Uuid), ()>,
}

impl AccessCache {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Warm the cache from the most recent grant per (granted_agent, target_agent) pair
    /// where that grant is approved.
    pub async fn load_from_db(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let cache = Self::new();

        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT granted_agent_id, target_agent_id
             FROM (
                 SELECT DISTINCT ON (granted_agent_id, target_agent_id)
                     granted_agent_id, target_agent_id, status
                 FROM a2a_access_grants
                 WHERE granted_agent_id IS NOT NULL
                 ORDER BY granted_agent_id, target_agent_id, requested_at DESC
             ) latest
             WHERE status = 'approved'",
        )
        .fetch_all(pool)
        .await?;

        let loaded = rows.len();
        for (granted_agent_id, target_agent_id) in rows {
            cache.inner.insert((granted_agent_id, target_agent_id), ());
        }

        tracing::info!(count = loaded, "Access grant cache warmed from DB");
        Ok(cache)
    }

    pub fn is_approved(&self, source_agent_id: Uuid, target_agent_id: Uuid) -> bool {
        self.inner.contains_key(&(source_agent_id, target_agent_id))
    }

    pub fn approve(&self, source_agent_id: Uuid, target_agent_id: Uuid) {
        self.inner.insert((source_agent_id, target_agent_id), ());
    }

    pub fn remove(&self, source_agent_id: Uuid, target_agent_id: Uuid) {
        self.inner.remove(&(source_agent_id, target_agent_id));
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_approve_and_check() {
        let cache = AccessCache::new();
        let agent_a = Uuid::new_v4();
        let agent_b = Uuid::new_v4();

        assert!(!cache.is_approved(agent_a, agent_b));

        cache.approve(agent_a, agent_b);
        assert!(cache.is_approved(agent_a, agent_b));
    }

    #[test]
    fn cache_remove() {
        let cache = AccessCache::new();
        let agent_a = Uuid::new_v4();
        let agent_b = Uuid::new_v4();

        cache.approve(agent_a, agent_b);
        assert_eq!(cache.len(), 1);

        cache.remove(agent_a, agent_b);
        assert!(!cache.is_approved(agent_a, agent_b));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_different_pairs_are_independent() {
        let cache = AccessCache::new();
        let agent_a = Uuid::new_v4();
        let agent_b = Uuid::new_v4();
        let agent_c = Uuid::new_v4();

        cache.approve(agent_a, agent_b);

        assert!(cache.is_approved(agent_a, agent_b));
        assert!(!cache.is_approved(agent_a, agent_c));
    }

    #[test]
    fn cache_is_directional() {
        let cache = AccessCache::new();
        let agent_a = Uuid::new_v4();
        let agent_b = Uuid::new_v4();

        cache.approve(agent_a, agent_b);

        assert!(cache.is_approved(agent_a, agent_b));
        assert!(!cache.is_approved(agent_b, agent_a));
    }

    #[test]
    fn cache_revoke_then_re_approve() {
        let cache = AccessCache::new();
        let agent_a = Uuid::new_v4();
        let agent_b = Uuid::new_v4();

        cache.approve(agent_a, agent_b);
        cache.remove(agent_a, agent_b);
        assert!(!cache.is_approved(agent_a, agent_b));

        cache.approve(agent_a, agent_b);
        assert!(cache.is_approved(agent_a, agent_b));
    }
}
