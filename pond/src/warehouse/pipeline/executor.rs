use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio::sync::watch;
use uuid::Uuid;

use reiver_core::events::{EventPublisher, PlatformEventType};
use gno_rs::wasm::udf::{OutputDescriptor, TableDescriptor};

use crate::warehouse::connectors::FetchOptions;
use crate::warehouse::sources::ConnectorRegistryService;
use crate::warehouse::udf::registry::UdfRegistry;
use crate::warehouse::udf::worker_pool::UdfWorkerPool;

use super::dag::topological_sort;
use super::store::PipelineStore;
use super::types::*;

pub struct PipelineExecutor {
    store: Arc<PipelineStore>,
    udf_registry: Arc<UdfRegistry>,
    worker_pool: Arc<UdfWorkerPool>,
    connector_registry: Arc<ConnectorRegistryService>,
    event_publisher: Option<Arc<EventPublisher>>,
}

impl PipelineExecutor {
    pub fn new(
        store: Arc<PipelineStore>,
        udf_registry: Arc<UdfRegistry>,
        worker_pool: Arc<UdfWorkerPool>,
        connector_registry: Arc<ConnectorRegistryService>,
    ) -> Self {
        Self {
            store,
            udf_registry,
            worker_pool,
            connector_registry,
            event_publisher: None,
        }
    }

    pub fn with_event_publisher(mut self, publisher: Arc<EventPublisher>) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    pub fn store(&self) -> &Arc<PipelineStore> {
        &self.store
    }

    pub async fn run(&self, project_id: Uuid, pipeline_id: Uuid, trigger: &str) -> Result<Uuid> {
        let pipeline = self
            .store
            .load(project_id, pipeline_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pipeline not found"))?;

        let sorted_ids = topological_sort(&pipeline.nodes, &pipeline.edges)
            .map_err(|e| anyhow::anyhow!("DAG validation failed: {}", e))?;

        let run_id = self
            .store
            .insert_run(pipeline_id, project_id, trigger)
            .await?;

        let node_map: HashMap<Uuid, &PipelineNode> =
            pipeline.nodes.iter().map(|n| (n.id, n)).collect();
        let adjacency_rev: HashMap<Uuid, Vec<Uuid>> = {
            let mut m: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
            for edge in &pipeline.edges {
                m.entry(edge.to_node_id)
                    .or_default()
                    .push(edge.from_node_id);
            }
            m
        };

        let mut buffers: HashMap<Uuid, Vec<RecordBatch>> = HashMap::new();
        let mut step_results = serde_json::Map::new();
        let mut incremental_cursors: Vec<(Uuid, String)> = Vec::new();

        let result: Result<()> = async {
            for &node_id in &sorted_ids {
                let node = node_map[&node_id];
                match &node.config {
                    NodeConfig::Source(src_cfg) => {
                        let batches = self
                            .execute_source(project_id, pipeline_id, node_id, src_cfg)
                            .await?;
                        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                        step_results.insert(
                            node_id.to_string(),
                            serde_json::json!({ "rows_read": row_count }),
                        );
                        buffers.insert(node_id, batches);
                    }
                    NodeConfig::Transform(tx_cfg) => {
                        let upstream_ids = adjacency_rev.get(&node_id).cloned().unwrap_or_default();
                        let mut input_batches: Vec<RecordBatch> = Vec::new();
                        for uid in &upstream_ids {
                            if let Some(bs) = buffers.get(uid) {
                                input_batches.extend(bs.iter().cloned());
                            }
                        }

                        let output_batches = self
                            .execute_transform(project_id, &input_batches, tx_cfg)
                            .await?;
                        let rows_in: usize = input_batches.iter().map(|b| b.num_rows()).sum();
                        let rows_out: usize = output_batches.iter().map(|b| b.num_rows()).sum();
                        step_results.insert(
                            node_id.to_string(),
                            serde_json::json!({ "rows_in": rows_in, "rows_out": rows_out }),
                        );
                        buffers.insert(node_id, output_batches);
                    }
                    NodeConfig::Sink(sink_cfg) => {
                        let upstream_ids = adjacency_rev.get(&node_id).cloned().unwrap_or_default();
                        let mut input_batches: Vec<RecordBatch> = Vec::new();
                        for uid in &upstream_ids {
                            if let Some(bs) = buffers.get(uid) {
                                input_batches.extend(bs.iter().cloned());
                            }
                        }

                        let rows_written = self
                            .execute_sink(project_id, &input_batches, sink_cfg)
                            .await?;
                        step_results.insert(
                            node_id.to_string(),
                            serde_json::json!({ "rows_written": rows_written }),
                        );
                    }
                }
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                for (node_id, last_val) in &incremental_cursors {
                    if let Err(e) = self
                        .store
                        .save_cursor(pipeline_id, *node_id, last_val)
                        .await
                    {
                        tracing::warn!(node_id = %node_id, error = %e, "Failed to save cursor");
                    }
                }
                let step_results_value = serde_json::Value::Object(step_results);
                self.store
                    .complete_run(run_id, Some(step_results_value.clone()))
                    .await?;

                if let Some(ref publisher) = self.event_publisher {
                    let _ = publisher.emit(
                        PlatformEventType::PipelineStepCompleted,
                        project_id,
                        format!("pipeline_step:{}:{}", pipeline_id, run_id),
                        serde_json::json!({
                            "pipeline_id": pipeline_id,
                            "run_id": run_id,
                            "trigger": trigger,
                            "steps": step_results_value,
                        }),
                    ).await;
                }
            }
            Err(ref e) => {
                if let Err(db_err) = self.store.fail_run(run_id, &e.to_string()).await {
                    tracing::error!(run_id = %run_id, error = %db_err, "Failed to record pipeline failure");
                }
                return Err(result.unwrap_err());
            }
        }

        Ok(run_id)
    }

    async fn execute_source(
        &self,
        project_id: Uuid,
        pipeline_id: Uuid,
        node_id: Uuid,
        cfg: &SourceNodeConfig,
    ) -> Result<Vec<RecordBatch>> {
        let connector = self
            .connector_registry
            .get_connector(project_id, &cfg.connector_name)
            .await
            .ok_or_else(|| {
                anyhow::anyhow!("source connector '{}' not found", cfg.connector_name)
            })?;

        match &cfg.read_strategy {
            ReadStrategy::Query { sql } => {
                if !connector.supports_sql_pushdown() {
                    anyhow::bail!(
                        "connector '{}' does not support SQL queries",
                        cfg.connector_name
                    );
                }
                connector
                    .execute_sql(sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("source query failed: {}", e))
            }
            ReadStrategy::Filter { table, filter } => {
                if !connector.supports_sql_pushdown() {
                    anyhow::bail!(
                        "connector '{}' does not support SQL-based filtering",
                        cfg.connector_name
                    );
                }
                let sql = format!("SELECT * FROM {} WHERE {}", table, filter);
                connector
                    .execute_sql(&sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("source filter query failed: {}", e))
            }
            ReadStrategy::Incremental { table, cursor_key } => {
                let last_value = self.store.load_cursor(pipeline_id, node_id).await?;
                let options = match last_value {
                    Some(ref lv) => FetchOptions::incremental(cursor_key.as_str(), lv.as_str()),
                    None => FetchOptions::default(),
                };
                let mut stream = connector
                    .fetch_table_stream(table, options)
                    .await
                    .map_err(|e| anyhow::anyhow!("source stream failed: {}", e))?;
                let mut batches = Vec::new();
                while let Some(batch_result) = stream.next().await {
                    batches.push(
                        batch_result.map_err(|e| anyhow::anyhow!("source stream error: {}", e))?,
                    );
                }
                Ok(batches)
            }
            ReadStrategy::FullSync { table } => {
                let mut stream = connector
                    .fetch_table_stream(table, FetchOptions::default())
                    .await
                    .map_err(|e| anyhow::anyhow!("source stream failed: {}", e))?;
                let mut batches = Vec::new();
                while let Some(batch_result) = stream.next().await {
                    batches.push(
                        batch_result.map_err(|e| anyhow::anyhow!("source stream error: {}", e))?,
                    );
                }
                Ok(batches)
            }
            ReadStrategy::BatchFetch {
                table,
                batch_size,
                max_rows,
            } => {
                let options = FetchOptions {
                    batch_size: Some(*batch_size),
                    max_rows: *max_rows,
                    ..Default::default()
                };
                let mut stream = connector
                    .fetch_table_stream(table, options)
                    .await
                    .map_err(|e| anyhow::anyhow!("source stream failed: {}", e))?;
                let mut batches = Vec::new();
                while let Some(batch_result) = stream.next().await {
                    batches.push(
                        batch_result.map_err(|e| anyhow::anyhow!("source stream error: {}", e))?,
                    );
                }
                Ok(batches)
            }
            ReadStrategy::CdcStream { .. } => {
                anyhow::bail!("CdcStream sources must use run_streaming(), not batch run()")
            }
        }
    }

    async fn execute_transform(
        &self,
        project_id: Uuid,
        input_batches: &[RecordBatch],
        cfg: &TransformNodeConfig,
    ) -> Result<Vec<RecordBatch>> {
        let compiled = self
            .udf_registry
            .get(project_id, &cfg.udf_name)
            .ok_or_else(|| anyhow::anyhow!("UDF '{}' not found", cfg.udf_name))?;

        let func_desc = compiled
            .manifest
            .functions
            .first()
            .ok_or_else(|| anyhow::anyhow!("UDF '{}' has no functions", cfg.udf_name))?;

        let input_schema = &func_desc.input;
        let output_schema = match &func_desc.output {
            OutputDescriptor::Table { fields } => TableDescriptor {
                fields: fields.clone(),
            },
            OutputDescriptor::Scalar { .. } => {
                anyhow::bail!("transform UDFs must return a table, not a scalar");
            }
        };

        let module = Arc::new(compiled.module.clone());
        let config_params = if cfg.params.is_empty() {
            None
        } else {
            Some(&cfg.params)
        };

        let mut output_batches = Vec::with_capacity(input_batches.len());
        for batch in input_batches {
            let output = self
                .worker_pool
                .process_batch_with_config(
                    &module,
                    batch.clone(),
                    input_schema,
                    &output_schema,
                    compiled.fuel_limit,
                    &func_desc.name,
                    compiled.timeout_secs,
                    config_params,
                )
                .await?;
            output_batches.push(output);
        }

        Ok(output_batches)
    }

    async fn execute_sink(
        &self,
        project_id: Uuid,
        batches: &[RecordBatch],
        cfg: &SinkNodeConfig,
    ) -> Result<u64> {
        let connector = self
            .connector_registry
            .get_connector(project_id, &cfg.connector_name)
            .await
            .ok_or_else(|| anyhow::anyhow!("sink connector '{}' not found", cfg.connector_name))?;

        if !connector.supports_write() {
            anyhow::bail!(
                "sink connector '{}' does not support writing",
                cfg.connector_name
            );
        }

        if !connector.supports_transactional_write() {
            tracing::warn!(
                sink = %cfg.connector_name,
                "Sink does not support transactional writes; partial data may be visible on failure"
            );
        }

        if batches.is_empty() {
            return Ok(0);
        }

        let result = connector
            .write_table(&cfg.table, batches.to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("sink write failed: {}", e))?;

        Ok(result.rows_written as u64)
    }

    pub async fn run_streaming(
        &self,
        project_id: Uuid,
        pipeline_id: Uuid,
        trigger: &str,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Uuid> {
        let pipeline = self
            .store
            .load(project_id, pipeline_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pipeline not found"))?;

        if pipeline.mode() != PipelineMode::Streaming {
            anyhow::bail!("run_streaming called on a non-streaming pipeline");
        }

        let sorted_ids = topological_sort(&pipeline.nodes, &pipeline.edges)
            .map_err(|e| anyhow::anyhow!("DAG validation failed: {}", e))?;

        let run_id = self
            .store
            .insert_run(pipeline_id, project_id, trigger)
            .await?;

        let node_map: HashMap<Uuid, &PipelineNode> =
            pipeline.nodes.iter().map(|n| (n.id, n)).collect();
        let adjacency_rev: HashMap<Uuid, Vec<Uuid>> = {
            let mut m: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
            for edge in &pipeline.edges {
                m.entry(edge.to_node_id)
                    .or_default()
                    .push(edge.from_node_id);
            }
            m
        };

        let source_nodes: Vec<(Uuid, &SourceNodeConfig)> = sorted_ids
            .iter()
            .filter_map(|id| {
                let node = node_map[id];
                if let NodeConfig::Source(cfg) = &node.config {
                    Some((node.id, cfg))
                } else {
                    None
                }
            })
            .collect();

        let downstream_ids: Vec<Uuid> = sorted_ids
            .iter()
            .filter(|id| !matches!(node_map[id].config, NodeConfig::Source(_)))
            .copied()
            .collect();

        let mut rows_read = 0i64;
        let mut rows_written = 0i64;
        let mut step_results = serde_json::Map::new();

        let result: Result<()> = async {
            for (source_id, src_cfg) in &source_nodes {
                let table = match &src_cfg.read_strategy {
                    ReadStrategy::CdcStream { table } => table.as_str(),
                    _ => anyhow::bail!("streaming pipeline source must use CdcStream strategy"),
                };

                let connector = self
                    .connector_registry
                    .get_connector(project_id, &src_cfg.connector_name)
                    .await
                    .ok_or_else(|| {
                        anyhow::anyhow!("source connector '{}' not found", src_cfg.connector_name)
                    })?;

                let checkpoint = self
                    .store
                    .load_cursor(pipeline_id, *source_id)
                    .await?
                    .unwrap_or_default();

                let mut stream = connector
                    .fetch_table_stream(table, FetchOptions::default())
                    .await
                    .map_err(|e| anyhow::anyhow!("source stream failed: {}", e))?;

                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            tracing::info!(pipeline_id = %pipeline_id, "Streaming pipeline shutting down");
                            break;
                        }
                        batch_opt = stream.next() => {
                            let Some(batch_result) = batch_opt else {
                                break;
                            };
                            let batch = batch_result
                                .map_err(|e| anyhow::anyhow!("source stream error: {}", e))?;
                            rows_read += batch.num_rows() as i64;

                            let mut buffers: HashMap<Uuid, Vec<RecordBatch>> = HashMap::new();
                            buffers.insert(*source_id, vec![batch]);

                            for &node_id in &downstream_ids {
                                let node = node_map[&node_id];
                                match &node.config {
                                    NodeConfig::Transform(tx_cfg) => {
                                        let upstream_ids = adjacency_rev.get(&node_id).cloned().unwrap_or_default();
                                        let mut input_batches: Vec<RecordBatch> = Vec::new();
                                        for uid in &upstream_ids {
                                            if let Some(bs) = buffers.get(uid) {
                                                input_batches.extend(bs.iter().cloned());
                                            }
                                        }
                                        let output_batches = self
                                            .execute_transform(project_id, &input_batches, tx_cfg)
                                            .await?;
                                        buffers.insert(node_id, output_batches);
                                    }
                                    NodeConfig::Sink(sink_cfg) => {
                                        let upstream_ids = adjacency_rev.get(&node_id).cloned().unwrap_or_default();
                                        let mut input_batches: Vec<RecordBatch> = Vec::new();
                                        for uid in &upstream_ids {
                                            if let Some(bs) = buffers.get(uid) {
                                                input_batches.extend(bs.iter().cloned());
                                            }
                                        }
                                        let written = self
                                            .execute_sink(project_id, &input_batches, sink_cfg)
                                            .await?;
                                        rows_written += written as i64;
                                    }
                                    NodeConfig::Source(_) => {}
                                }
                            }

                            let _ = checkpoint.as_str();
                            if let Err(e) = self
                                .store
                                .save_cursor(pipeline_id, *source_id, &rows_read.to_string())
                                .await
                            {
                                tracing::warn!(error = %e, "Failed to save streaming checkpoint");
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        .await;

        step_results.insert(
            "totals".to_string(),
            serde_json::json!({ "rows_read": rows_read, "rows_written": rows_written }),
        );

        match result {
            Ok(()) => {
                let step_results_value = serde_json::Value::Object(step_results);
                self.store
                    .complete_run(run_id, Some(step_results_value.clone()))
                    .await?;

                if let Some(ref publisher) = self.event_publisher {
                    let _ = publisher.emit(
                        PlatformEventType::PipelineStepCompleted,
                        project_id,
                        format!("pipeline_step:{}:{}", pipeline_id, run_id),
                        serde_json::json!({
                            "pipeline_id": pipeline_id,
                            "run_id": run_id,
                            "trigger": trigger,
                            "mode": "streaming",
                            "steps": step_results_value,
                        }),
                    ).await;
                }
            }
            Err(ref e) => {
                if let Err(db_err) = self.store.fail_run(run_id, &e.to_string()).await {
                    tracing::error!(run_id = %run_id, error = %db_err, "Failed to record streaming pipeline failure");
                }
                return Err(result.unwrap_err());
            }
        }

        Ok(run_id)
    }
}
