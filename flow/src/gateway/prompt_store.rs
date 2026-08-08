//! Prompt Store abstraction for the AI Gateway.
//!
//! Separates prompt hub infrastructure access (Postgres + Redis) from the
//! resolution and variant-assignment logic in [`super::prompt_resolver`].

use async_trait::async_trait;
use bb8_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::gateway::domain_types::RolloutStatus;

use super::prompt_resolver::{ActiveRollout, ActiveRolloutRow, PromptVersionConfig};

/// Lookup result for a prompt config by name.
#[derive(Debug, Clone)]
pub struct PromptConfigRow {
    pub id: Uuid,
    pub active_version_id: Option<Uuid>,
}

/// Timeout for Redis cache lookup operations (milliseconds).
const CACHE_LOOKUP_TIMEOUT_MS: u64 = 100;

/// Cache TTL for active rollouts (30 seconds -- short because weights change).
const ROLLOUT_CACHE_TTL_SECONDS: u64 = 30;

/// Cache TTL for version configs (5 minutes -- immutable once created).
const VERSION_CACHE_TTL_SECONDS: u64 = 300;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Abstraction over prompt hub storage (configs, versions, rollouts).
///
/// The production implementation ([`PgPromptStore`]) uses Postgres with a
/// Redis caching layer.  Tests can substitute [`InMemoryPromptStore`] to
/// exercise the full resolution path without any infrastructure.
#[async_trait]
pub trait PromptStore: Send + Sync {
    /// Look up a prompt config by project + name.
    async fn get_config_by_name(
        &self,
        project_id: Uuid,
        name: &str,
    ) -> Option<PromptConfigRow>;

    /// Fetch the running rollout for a config (if any).
    async fn get_active_rollout(&self, config_id: Uuid) -> Option<ActiveRollout>;

    /// Fetch a prompt version's full configuration.
    async fn get_version_config(&self, version_id: Uuid) -> Option<PromptVersionConfig>;
}

// ---------------------------------------------------------------------------
// Production implementation: Postgres + Redis cache
// ---------------------------------------------------------------------------

/// Postgres-backed [`PromptStore`] with Redis caching.
pub struct PgPromptStore {
    db: PgPool,
    redis: RedisPool,
}

impl PgPromptStore {
    pub fn new(db: PgPool, redis: RedisPool) -> Self {
        Self { db, redis }
    }
}

#[async_trait]
impl PromptStore for PgPromptStore {
    async fn get_config_by_name(
        &self,
        project_id: Uuid,
        name: &str,
    ) -> Option<PromptConfigRow> {
        let row: (Uuid, Option<Uuid>) = sqlx::query_as(
            r#"
            SELECT id, active_version_id
            FROM llm_prompt_configs
            WHERE project_id = $1 AND name = $2
            "#,
        )
        .bind(project_id)
        .bind(name)
        .fetch_optional(&self.db)
        .await
        .ok()
        .flatten()?;

        Some(PromptConfigRow {
            id: row.0,
            active_version_id: row.1,
        })
    }

    async fn get_active_rollout(&self, config_id: Uuid) -> Option<ActiveRollout> {
        let cache_key = format!("rollout:config:{}", config_id);

        if let Some(cached) = get_from_cache::<ActiveRollout>(&self.redis, &cache_key).await {
            return Some(cached);
        }

        let rollout: Option<ActiveRollout> = sqlx::query_as::<_, ActiveRolloutRow>(&format!(
            r#"
            SELECT id, config_id, target_version_id, baseline_version_id,
                   current_weight, allocation_type
            FROM llm_rollouts
            WHERE config_id = $1 AND status = '{}'
            "#,
            RolloutStatus::Running.as_str()
        ))
        .bind(config_id)
        .fetch_optional(&self.db)
        .await
        .ok()
        .flatten()
        .map(ActiveRollout::from);

        if let Some(ref rollout) = rollout {
            set_in_cache(&self.redis, &cache_key, rollout, ROLLOUT_CACHE_TTL_SECONDS).await;
        }

        rollout
    }

    async fn get_version_config(&self, version_id: Uuid) -> Option<PromptVersionConfig> {
        let cache_key = format!("prompt_version:{}", version_id);

        if let Some(cached) = get_from_cache::<PromptVersionConfig>(&self.redis, &cache_key).await {
            return Some(cached);
        }

        let version = get_version_from_db(&self.db, version_id).await;

        if let Some(ref version) = version {
            set_in_cache(&self.redis, &cache_key, version, VERSION_CACHE_TTL_SECONDS).await;
        }

        version
    }
}

// ---------------------------------------------------------------------------
// Shared helpers (Redis cache + raw DB query)
// ---------------------------------------------------------------------------

async fn get_from_cache<T: for<'de> Deserialize<'de>>(redis: &RedisPool, key: &str) -> Option<T> {
    let mut conn = redis.get().await.ok()?;

    let cached: Option<String> = tokio::time::timeout(
        Duration::from_millis(CACHE_LOOKUP_TIMEOUT_MS),
        conn.get(key),
    )
    .await
    .ok()
    .and_then(|r| r.ok())
    .flatten();

    cached.and_then(|s| serde_json::from_str(&s).ok())
}

async fn set_in_cache<T: Serialize>(redis: &RedisPool, key: &str, value: &T, ttl_seconds: u64) {
    if let Ok(mut conn) = redis.get().await {
        if let Ok(json) = serde_json::to_string(value) {
            let _ = tokio::time::timeout(
                Duration::from_millis(CACHE_LOOKUP_TIMEOUT_MS),
                conn.set_ex::<_, _, ()>(key, json, ttl_seconds),
            )
            .await;
        }
    }
}

async fn get_version_from_db(db: &PgPool, version_id: Uuid) -> Option<PromptVersionConfig> {
    sqlx::query_as(
        r#"
        SELECT id, system_prompt, model, temperature, max_tokens, variables, tools,
               response_format, parameters, allowed_tools
        FROM llm_prompt_versions
        WHERE id = $1
        "#,
    )
    .bind(version_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

// ---------------------------------------------------------------------------
// Write trait extension
// ---------------------------------------------------------------------------

/// Write result for config creation/update.
#[derive(Debug, Clone)]
pub struct WriteConfigResult {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub active_version_id: Option<Uuid>,
}

/// Write operations for the prompt hub.
///
/// Extends [`PromptStore`] with mutations used by the management API.
/// Production uses [`PgPromptStore`]; tests use [`InMemoryPromptStore`].
#[async_trait]
pub trait PromptWriteStore: PromptStore {
    /// Check whether a config with the given name already exists in the project.
    async fn config_name_exists(&self, project_id: Uuid, name: &str) -> bool;

    /// Insert a new prompt config.  Returns the created row.
    async fn create_config(
        &self,
        project_id: Uuid,
        name: &str,
        description: Option<&str>,
    ) -> anyhow::Result<WriteConfigResult>;

    /// Update a prompt config's name and/or description.
    async fn update_config(
        &self,
        config_id: Uuid,
        project_id: Uuid,
        name: Option<&str>,
        description: Option<&str>,
    ) -> anyhow::Result<Option<WriteConfigResult>>;

    /// Delete a prompt config.  Returns `true` if a row was removed.
    async fn delete_config(&self, config_id: Uuid, project_id: Uuid) -> anyhow::Result<bool>;

    /// Set the active version for a config.
    async fn set_active_version(
        &self,
        config_id: Uuid,
        version_id: Uuid,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl PromptWriteStore for PgPromptStore {
    async fn config_name_exists(&self, project_id: Uuid, name: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM llm_prompt_configs WHERE project_id = $1 AND name = $2)",
        )
        .bind(project_id)
        .bind(name)
        .fetch_one(&self.db)
        .await
        .unwrap_or(false)
    }

    async fn create_config(
        &self,
        project_id: Uuid,
        name: &str,
        description: Option<&str>,
    ) -> anyhow::Result<WriteConfigResult> {
        let row: (Uuid, Uuid, String, Option<Uuid>) = sqlx::query_as(
            r#"
            INSERT INTO llm_prompt_configs (project_id, name, description)
            VALUES ($1, $2, $3)
            RETURNING id, project_id, name, active_version_id
            "#,
        )
        .bind(project_id)
        .bind(name)
        .bind(description)
        .fetch_one(&self.db)
        .await?;

        Ok(WriteConfigResult {
            id: row.0,
            project_id: row.1,
            name: row.2,
            active_version_id: row.3,
        })
    }

    async fn update_config(
        &self,
        config_id: Uuid,
        project_id: Uuid,
        name: Option<&str>,
        description: Option<&str>,
    ) -> anyhow::Result<Option<WriteConfigResult>> {
        let row: Option<(Uuid, Uuid, String, Option<Uuid>)> = sqlx::query_as(
            r#"
            UPDATE llm_prompt_configs
            SET name = COALESCE($3, name),
                description = COALESCE($4, description),
                updated_at = NOW()
            WHERE id = $1 AND project_id = $2
            RETURNING id, project_id, name, active_version_id
            "#,
        )
        .bind(config_id)
        .bind(project_id)
        .bind(name)
        .bind(description)
        .fetch_optional(&self.db)
        .await?;

        Ok(row.map(|r| WriteConfigResult {
            id: r.0,
            project_id: r.1,
            name: r.2,
            active_version_id: r.3,
        }))
    }

    async fn delete_config(&self, config_id: Uuid, project_id: Uuid) -> anyhow::Result<bool> {
        let result =
            sqlx::query("DELETE FROM llm_prompt_configs WHERE id = $1 AND project_id = $2")
                .bind(config_id)
                .bind(project_id)
                .execute(&self.db)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn set_active_version(
        &self,
        config_id: Uuid,
        version_id: Uuid,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE llm_prompt_configs SET active_version_id = $1 WHERE id = $2")
            .bind(version_id)
            .bind(config_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory implementation for tests
// ---------------------------------------------------------------------------

/// HashMap-backed [`PromptStore`] + [`PromptWriteStore`] for tests.
///
/// Holds pre-seeded configs, rollouts, and version configs so that
/// [`super::prompt_resolver::resolve_prompt_config`] and the management
/// API validation logic can be exercised without any database or Redis.
pub struct InMemoryPromptStore {
    configs: std::collections::HashMap<(Uuid, String), PromptConfigRow>,
    rollouts: std::collections::HashMap<Uuid, ActiveRollout>,
    versions: std::collections::HashMap<Uuid, PromptVersionConfig>,
    configs_by_id: std::collections::HashMap<Uuid, (Uuid, String)>,
}

impl InMemoryPromptStore {
    pub fn new() -> Self {
        Self {
            configs: std::collections::HashMap::new(),
            rollouts: std::collections::HashMap::new(),
            versions: std::collections::HashMap::new(),
            configs_by_id: std::collections::HashMap::new(),
        }
    }

    pub fn add_config(&mut self, project_id: Uuid, name: &str, row: PromptConfigRow) {
        self.configs_by_id
            .insert(row.id, (project_id, name.to_string()));
        self.configs.insert((project_id, name.to_string()), row);
    }

    pub fn add_rollout(&mut self, config_id: Uuid, rollout: ActiveRollout) {
        self.rollouts.insert(config_id, rollout);
    }

    pub fn add_version(&mut self, version: PromptVersionConfig) {
        self.versions.insert(version.id, version.clone());
    }
}

#[async_trait]
impl PromptStore for InMemoryPromptStore {
    async fn get_config_by_name(
        &self,
        project_id: Uuid,
        name: &str,
    ) -> Option<PromptConfigRow> {
        self.configs.get(&(project_id, name.to_string())).cloned()
    }

    async fn get_active_rollout(&self, config_id: Uuid) -> Option<ActiveRollout> {
        self.rollouts.get(&config_id).cloned()
    }

    async fn get_version_config(&self, version_id: Uuid) -> Option<PromptVersionConfig> {
        self.versions.get(&version_id).cloned()
    }
}

#[async_trait]
impl PromptWriteStore for InMemoryPromptStore {
    async fn config_name_exists(&self, project_id: Uuid, name: &str) -> bool {
        self.configs.contains_key(&(project_id, name.to_string()))
    }

    async fn create_config(
        &self,
        project_id: Uuid,
        name: &str,
        _description: Option<&str>,
    ) -> anyhow::Result<WriteConfigResult> {
        if self
            .configs
            .contains_key(&(project_id, name.to_string()))
        {
            anyhow::bail!("Duplicate config name");
        }
        let id = Uuid::new_v4();
        Ok(WriteConfigResult {
            id,
            project_id,
            name: name.to_string(),
            active_version_id: None,
        })
    }

    async fn update_config(
        &self,
        config_id: Uuid,
        _project_id: Uuid,
        name: Option<&str>,
        _description: Option<&str>,
    ) -> anyhow::Result<Option<WriteConfigResult>> {
        let key = self.configs_by_id.get(&config_id);
        match key {
            Some((pid, old_name)) => Ok(Some(WriteConfigResult {
                id: config_id,
                project_id: *pid,
                name: name.unwrap_or(old_name).to_string(),
                active_version_id: self
                    .configs
                    .get(&(*pid, old_name.clone()))
                    .and_then(|r| r.active_version_id),
            })),
            None => Ok(None),
        }
    }

    async fn delete_config(&self, config_id: Uuid, _project_id: Uuid) -> anyhow::Result<bool> {
        Ok(self.configs_by_id.contains_key(&config_id))
    }

    async fn set_active_version(
        &self,
        _config_id: Uuid,
        _version_id: Uuid,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
