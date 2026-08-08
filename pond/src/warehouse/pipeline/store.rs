use std::sync::Arc;

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::db::DbPool;

use super::types::*;

pub struct PipelineStore {
    db: Arc<DbPool>,
}

impl PipelineStore {
    pub fn new(db: Arc<DbPool>) -> Self {
        Self { db }
    }

    pub async fn list(&self, project_id: Uuid) -> Result<Vec<PipelineSummary>> {
        let rows = sqlx::query_as::<_, (Uuid, String, Option<String>, Option<String>, bool, String, i64, Option<String>, Option<chrono::DateTime<chrono::Utc>>, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT p.id, p.name, p.description, p.schedule, p.enabled, p.mode,
                   (SELECT COUNT(*) FROM warehouse_pipeline_nodes WHERE pipeline_id = p.id) AS node_count,
                   lr.status AS last_run_status,
                   lr.created_at AS last_run_at,
                   p.created_at, p.updated_at
            FROM warehouse_pipelines p
            LEFT JOIN LATERAL (
                SELECT status, created_at FROM warehouse_pipeline_runs
                WHERE pipeline_id = p.id ORDER BY created_at DESC LIMIT 1
            ) lr ON true
            WHERE p.project_id = $1
            ORDER BY p.name
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to list pipelines")?;

        Ok(rows
            .into_iter()
            .map(
                |(id, name, description, schedule, enabled, mode, node_count, last_run_status, last_run_at, created_at, updated_at)| {
                    PipelineSummary {
                        id,
                        name,
                        description,
                        schedule,
                        enabled,
                        mode,
                        node_count,
                        last_run_status,
                        last_run_at,
                        created_at,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    pub async fn load(&self, project_id: Uuid, pipeline_id: Uuid) -> Result<Option<Pipeline>> {
        let row = sqlx::query_as::<
            _,
            (
                Uuid,
                Uuid,
                String,
                Option<String>,
                Option<String>,
                bool,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            r#"
            SELECT id, project_id, name, description, schedule, enabled, created_at, updated_at
            FROM warehouse_pipelines
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(pipeline_id)
        .bind(project_id)
        .fetch_optional(self.db.as_ref())
        .await
        .context("failed to load pipeline")?;

        let Some((id, proj_id, name, description, schedule, enabled, created_at, updated_at)) = row
        else {
            return Ok(None);
        };

        let nodes = self.load_nodes(pipeline_id).await?;
        let edges = self.load_edges(pipeline_id).await?;

        Ok(Some(Pipeline {
            id,
            project_id: proj_id,
            name,
            description,
            schedule,
            enabled,
            nodes,
            edges,
            created_at,
            updated_at,
        }))
    }

    async fn load_nodes(&self, pipeline_id: Uuid) -> Result<Vec<PipelineNode>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, serde_json::Value, f32, f32)>(
            r#"
            SELECT id, pipeline_id, node_type, label, config, position_x, position_y
            FROM warehouse_pipeline_nodes
            WHERE pipeline_id = $1
            "#,
        )
        .bind(pipeline_id)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to load pipeline nodes")?;

        rows.into_iter()
            .map(|(id, pid, node_type_str, label, config_json, px, py)| {
                let node_type = NodeType::from_str(&node_type_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown node type: {}", node_type_str))?;
                let config: NodeConfig = serde_json::from_value(config_json)
                    .context("failed to deserialize node config")?;
                Ok(PipelineNode {
                    id,
                    pipeline_id: pid,
                    node_type,
                    label,
                    config,
                    position_x: px,
                    position_y: py,
                })
            })
            .collect()
    }

    async fn load_edges(&self, pipeline_id: Uuid) -> Result<Vec<PipelineEdge>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, Uuid, Uuid)>(
            r#"
            SELECT id, pipeline_id, from_node_id, to_node_id
            FROM warehouse_pipeline_edges
            WHERE pipeline_id = $1
            "#,
        )
        .bind(pipeline_id)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to load pipeline edges")?;

        Ok(rows
            .into_iter()
            .map(|(id, pid, from_id, to_id)| PipelineEdge {
                id,
                pipeline_id: pid,
                from_node_id: from_id,
                to_node_id: to_id,
            })
            .collect())
    }

    /// Save a new pipeline. Inserts the pipeline row + all nodes + all edges in
    /// a single transaction. Returns the generated pipeline ID.
    pub async fn create(&self, project_id: Uuid, payload: &PipelineGraphPayload) -> Result<Uuid> {
        let mut tx = self
            .db
            .begin()
            .await
            .context("failed to start transaction")?;

        let mode = payload.compute_mode();
        let pipeline_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO warehouse_pipelines (project_id, name, description, schedule, enabled, mode)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(project_id)
        .bind(&payload.name)
        .bind(&payload.description)
        .bind(&payload.schedule)
        .bind(payload.enabled.unwrap_or(true))
        .bind(mode.as_str())
        .fetch_one(&mut *tx)
        .await
        .context("failed to insert pipeline")?;

        self.insert_nodes_edges(&mut tx, pipeline_id, payload)
            .await?;

        tx.commit()
            .await
            .context("failed to commit pipeline creation")?;
        Ok(pipeline_id)
    }

    /// Full-replace update: delete old nodes/edges and re-insert from the payload.
    pub async fn update(
        &self,
        project_id: Uuid,
        pipeline_id: Uuid,
        payload: &PipelineGraphPayload,
    ) -> Result<()> {
        let mut tx = self
            .db
            .begin()
            .await
            .context("failed to start transaction")?;

        let mode = payload.compute_mode();
        let affected = sqlx::query(
            r#"
            UPDATE warehouse_pipelines
            SET name = $3, description = $4, schedule = $5, enabled = $6, mode = $7, updated_at = NOW()
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(pipeline_id)
        .bind(project_id)
        .bind(&payload.name)
        .bind(&payload.description)
        .bind(&payload.schedule)
        .bind(payload.enabled.unwrap_or(true))
        .bind(mode.as_str())
        .execute(&mut *tx)
        .await
        .context("failed to update pipeline")?
        .rows_affected();

        if affected == 0 {
            anyhow::bail!("pipeline not found");
        }

        // CASCADE on edges handled by FK, but we also need to clear nodes
        sqlx::query("DELETE FROM warehouse_pipeline_edges WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM warehouse_pipeline_nodes WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .execute(&mut *tx)
            .await?;

        self.insert_nodes_edges(&mut tx, pipeline_id, payload)
            .await?;

        tx.commit()
            .await
            .context("failed to commit pipeline update")?;
        Ok(())
    }

    async fn insert_nodes_edges(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        pipeline_id: Uuid,
        payload: &PipelineGraphPayload,
    ) -> Result<()> {
        for node in &payload.nodes {
            let config_json =
                serde_json::to_value(&node.config).context("failed to serialize node config")?;
            sqlx::query(
                r#"
                INSERT INTO warehouse_pipeline_nodes (id, pipeline_id, node_type, label, config, position_x, position_y)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(node.id)
            .bind(pipeline_id)
            .bind(node.node_type.as_str())
            .bind(&node.label)
            .bind(&config_json)
            .bind(node.position_x)
            .bind(node.position_y)
            .execute(&mut **tx)
            .await
            .context("failed to insert pipeline node")?;
        }

        for edge in &payload.edges {
            sqlx::query(
                r#"
                INSERT INTO warehouse_pipeline_edges (pipeline_id, from_node_id, to_node_id)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(pipeline_id)
            .bind(edge.from_node_id)
            .bind(edge.to_node_id)
            .execute(&mut **tx)
            .await
            .context("failed to insert pipeline edge")?;
        }

        Ok(())
    }

    pub async fn delete(&self, project_id: Uuid, pipeline_id: Uuid) -> Result<bool> {
        let result =
            sqlx::query("DELETE FROM warehouse_pipelines WHERE id = $1 AND project_id = $2")
                .bind(pipeline_id)
                .bind(project_id)
                .execute(self.db.as_ref())
                .await
                .context("failed to delete pipeline")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn insert_run(
        &self,
        pipeline_id: Uuid,
        project_id: Uuid,
        trigger: &str,
    ) -> Result<Uuid> {
        let run_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO warehouse_pipeline_runs (pipeline_id, project_id, status, trigger, started_at)
            VALUES ($1, $2, 'running', $3, NOW())
            RETURNING id
            "#,
        )
        .bind(pipeline_id)
        .bind(project_id)
        .bind(trigger)
        .fetch_one(self.db.as_ref())
        .await
        .context("failed to insert pipeline run")?;

        Ok(run_id)
    }

    pub async fn complete_run(
        &self,
        run_id: Uuid,
        step_results: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE warehouse_pipeline_runs
            SET status = 'succeeded', finished_at = NOW(), step_results = $2
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(&step_results)
        .execute(self.db.as_ref())
        .await
        .context("failed to complete pipeline run")?;
        Ok(())
    }

    pub async fn fail_run(&self, run_id: Uuid, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE warehouse_pipeline_runs
            SET status = 'failed', finished_at = NOW(), error_message = $2
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(error)
        .execute(self.db.as_ref())
        .await
        .context("failed to update pipeline run to failed")?;
        Ok(())
    }

    pub async fn get_runs(
        &self,
        project_id: Uuid,
        pipeline_id: Uuid,
    ) -> Result<Vec<PipelineRunInfo>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>, Option<String>, Option<serde_json::Value>, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT id, pipeline_id, status, trigger, started_at, finished_at, error_message, step_results, created_at
            FROM warehouse_pipeline_runs
            WHERE pipeline_id = $1 AND project_id = $2
            ORDER BY created_at DESC
            LIMIT 50
            "#,
        )
        .bind(pipeline_id)
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to get pipeline runs")?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    pid,
                    status,
                    trigger,
                    started_at,
                    finished_at,
                    error_message,
                    step_results,
                    created_at,
                )| {
                    PipelineRunInfo {
                        id,
                        pipeline_id: pid,
                        status,
                        trigger,
                        started_at,
                        finished_at,
                        error_message,
                        step_results,
                        created_at,
                    }
                },
            )
            .collect())
    }

    pub async fn load_cursor(&self, pipeline_id: Uuid, node_id: Uuid) -> Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT last_value FROM warehouse_pipeline_cursors WHERE pipeline_id = $1 AND node_id = $2",
        )
        .bind(pipeline_id)
        .bind(node_id)
        .fetch_optional(self.db.as_ref())
        .await
        .context("failed to load cursor")?;

        Ok(row.and_then(|(v,)| v))
    }

    pub async fn save_cursor(
        &self,
        pipeline_id: Uuid,
        node_id: Uuid,
        last_value: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO warehouse_pipeline_cursors (pipeline_id, node_id, last_value, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (pipeline_id, node_id) DO UPDATE
            SET last_value = EXCLUDED.last_value, updated_at = NOW()
            "#,
        )
        .bind(pipeline_id)
        .bind(node_id)
        .bind(last_value)
        .execute(self.db.as_ref())
        .await
        .context("failed to save cursor")?;
        Ok(())
    }

    pub async fn cleanup_stale_runs(&self, before: chrono::DateTime<chrono::Utc>) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE warehouse_pipeline_runs SET status = 'crashed', finished_at = NOW(), \
             error_message = 'Server restarted during execution' \
             WHERE status = 'running' AND started_at < $1",
        )
        .bind(before)
        .execute(self.db.as_ref())
        .await?;

        let rows = result.rows_affected();
        if rows > 0 {
            tracing::warn!(count = rows, "Marked stale pipeline runs as crashed");
        }
        Ok(rows)
    }

    pub async fn list_scheduled(&self) -> Result<Vec<(Uuid, Uuid, String, String)>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String)>(
            r#"
            SELECT id, project_id, name, schedule
            FROM warehouse_pipelines
            WHERE enabled = true AND schedule IS NOT NULL
            "#,
        )
        .fetch_all(self.db.as_ref())
        .await
        .context("failed to list scheduled pipelines")?;
        Ok(rows)
    }
}
