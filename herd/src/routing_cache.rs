use dashmap::DashMap;
use sqlx::PgPool;
use uuid::Uuid;

/// Cached routing info for a registered A2A agent.
#[derive(Debug, Clone)]
pub struct AgentRouting {
    pub endpoint_url: String,
    pub enabled: bool,
    pub webhook_secret: Option<String>,
}

/// Cached push notification config for a task subscription.
#[derive(Debug, Clone)]
pub struct CachedPushConfig {
    pub id: Uuid,
    pub webhook_url: String,
    pub auth_scheme: Option<String>,
    pub auth_credentials: Option<String>,
}

/// In-memory cache of agent routing data and push configs, eliminating
/// Postgres from the message-forwarding hot path.
///
/// Warmed from the DB on startup and kept in sync by all mutation paths
/// (agent create/update, org webhook-secret changes, push config CRUD).
pub struct RoutingCache {
    agents: DashMap<Uuid, AgentRouting>,
    /// Push configs keyed by task_id string (as stored in Kafka envelopes).
    push_configs: DashMap<String, Vec<CachedPushConfig>>,
}

impl RoutingCache {
    pub fn new() -> Self {
        Self {
            agents: DashMap::new(),
            push_configs: DashMap::new(),
        }
    }

    /// Warm both agent routing and push config caches from Postgres.
    pub async fn load_from_db(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let cache = Self::new();

        // Agent routing: join through projects → organizations to get webhook_secret
        let agent_rows: Vec<(Uuid, String, bool, Option<String>)> = sqlx::query_as(
            "SELECT a.id, a.endpoint_url, a.enabled, o.webhook_secret
             FROM a2a_agents a
             JOIN projects p ON p.id = a.project_id
             JOIN organizations o ON o.id = p.organization_id",
        )
        .fetch_all(pool)
        .await?;

        let agent_count = agent_rows.len();
        for (agent_id, endpoint_url, enabled, webhook_secret) in agent_rows {
            cache.agents.insert(
                agent_id,
                AgentRouting {
                    endpoint_url,
                    enabled,
                    webhook_secret,
                },
            );
        }

        // Push configs
        let push_rows: Vec<(Uuid, String, String, Option<String>, Option<String>)> =
            sqlx::query_as(
                "SELECT id, task_id, webhook_url, auth_scheme, auth_credentials
                 FROM a2a_push_configs",
            )
            .fetch_all(pool)
            .await?;

        let push_count = push_rows.len();
        for (id, task_id, webhook_url, auth_scheme, auth_credentials) in push_rows {
            cache
                .push_configs
                .entry(task_id)
                .or_default()
                .push(CachedPushConfig {
                    id,
                    webhook_url,
                    auth_scheme,
                    auth_credentials,
                });
        }

        tracing::info!(
            agents = agent_count,
            push_configs = push_count,
            "Routing cache warmed from DB"
        );
        Ok(cache)
    }

    // ── Agent routing lookups ──

    pub fn get_agent(&self, agent_id: Uuid) -> Option<AgentRouting> {
        self.agents.get(&agent_id).map(|v| v.clone())
    }

    /// Called after INSERT/UPDATE on a2a_agents.
    pub fn upsert_agent(&self, agent_id: Uuid, routing: AgentRouting) {
        self.agents.insert(agent_id, routing);
    }

    /// Called after DELETE on a2a_agents.
    pub fn remove_agent(&self, agent_id: Uuid) {
        self.agents.remove(&agent_id);
    }

    /// Update just the webhook_secret for every agent belonging to an org.
    pub fn update_org_webhook_secret(
        &self,
        org_id: Uuid,
        secret: Option<String>,
        pool_agents: &[(Uuid, Uuid)],
    ) {
        // pool_agents is not needed if we store org_id; instead we just
        // accept a list of (agent_id, org_id) pairs to update.
        // Simpler: caller passes the affected agent IDs.
        let _ = org_id;
        for (agent_id, _) in pool_agents {
            if let Some(mut entry) = self.agents.get_mut(agent_id) {
                entry.webhook_secret = secret.clone();
            }
        }
    }

    /// Bulk-update webhook_secret for all cached agents belonging to an org.
    /// Iterates all cached agents (cheap for typical fleet sizes).
    pub fn set_webhook_secret_for_org(&self, secret: Option<String>, agent_ids: &[Uuid]) {
        for agent_id in agent_ids {
            if let Some(mut entry) = self.agents.get_mut(agent_id) {
                entry.webhook_secret = secret.clone();
            }
        }
    }

    // ── Push config lookups ──

    pub fn get_push_configs(&self, task_id: &str) -> Vec<CachedPushConfig> {
        self.push_configs
            .get(task_id)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Called after INSERT on a2a_push_configs.
    pub fn insert_push_config(&self, task_id: String, config: CachedPushConfig) {
        self.push_configs.entry(task_id).or_default().push(config);
    }

    /// Called after DELETE on a2a_push_configs.
    pub fn remove_push_config(&self, task_id: &str, config_id: Uuid) {
        if let Some(mut configs) = self.push_configs.get_mut(task_id) {
            configs.retain(|c| c.id != config_id);
        }
    }

    #[cfg(test)]
    pub fn agent_count(&self) -> usize {
        self.agents.len()
    }

    #[cfg(test)]
    pub fn push_config_count(&self) -> usize {
        self.push_configs.iter().map(|e| e.value().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_upsert_and_lookup() {
        let cache = RoutingCache::new();
        let id = Uuid::new_v4();

        assert!(cache.get_agent(id).is_none());

        cache.upsert_agent(
            id,
            AgentRouting {
                endpoint_url: "https://example.com/a2a".into(),
                enabled: true,
                webhook_secret: Some("secret".into()),
            },
        );

        let routing = cache.get_agent(id).unwrap();
        assert_eq!(routing.endpoint_url, "https://example.com/a2a");
        assert!(routing.enabled);
        assert_eq!(routing.webhook_secret.as_deref(), Some("secret"));
    }

    #[test]
    fn agent_disabled_is_still_cached() {
        let cache = RoutingCache::new();
        let id = Uuid::new_v4();

        cache.upsert_agent(
            id,
            AgentRouting {
                endpoint_url: "https://example.com/a2a".into(),
                enabled: false,
                webhook_secret: None,
            },
        );

        let routing = cache.get_agent(id).unwrap();
        assert!(!routing.enabled);
    }

    #[test]
    fn push_config_crud() {
        let cache = RoutingCache::new();
        let task = "task-1".to_string();
        let cfg_id = Uuid::new_v4();

        assert!(cache.get_push_configs(&task).is_empty());

        cache.insert_push_config(
            task.clone(),
            CachedPushConfig {
                id: cfg_id,
                webhook_url: "https://hook.example.com".into(),
                auth_scheme: None,
                auth_credentials: None,
            },
        );

        let configs = cache.get_push_configs(&task);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].webhook_url, "https://hook.example.com");

        cache.remove_push_config(&task, cfg_id);
        assert!(cache.get_push_configs(&task).is_empty());
    }

    #[test]
    fn webhook_secret_bulk_update() {
        let cache = RoutingCache::new();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let a3 = Uuid::new_v4();

        for id in [a1, a2, a3] {
            cache.upsert_agent(
                id,
                AgentRouting {
                    endpoint_url: "https://x.com".into(),
                    enabled: true,
                    webhook_secret: None,
                },
            );
        }

        cache.set_webhook_secret_for_org(Some("new-secret".into()), &[a1, a3]);

        assert_eq!(
            cache.get_agent(a1).unwrap().webhook_secret.as_deref(),
            Some("new-secret")
        );
        assert!(cache.get_agent(a2).unwrap().webhook_secret.is_none());
        assert_eq!(
            cache.get_agent(a3).unwrap().webhook_secret.as_deref(),
            Some("new-secret")
        );
    }
}
