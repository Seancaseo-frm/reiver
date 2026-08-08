use std::sync::Arc;

use anyhow::Result;

use crate::warehouse::udf::registry::UdfRegistry;

use super::events::{EventStore, EventType};
use super::store::PipelineStore;

pub struct CronEmitter {
    event_store: Arc<EventStore>,
    pipeline_store: Arc<PipelineStore>,
    udf_registry: Option<Arc<UdfRegistry>>,
    scheduler: tokio::sync::Mutex<Option<tokio_cron_scheduler::JobScheduler>>,
}

impl CronEmitter {
    pub fn new(
        event_store: Arc<EventStore>,
        pipeline_store: Arc<PipelineStore>,
        udf_registry: Option<Arc<UdfRegistry>>,
    ) -> Self {
        Self {
            event_store,
            pipeline_store,
            udf_registry,
            scheduler: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn schedule_all(self: &Arc<Self>) -> Result<()> {
        use tokio_cron_scheduler::{Job, JobScheduler};

        let sched = JobScheduler::new()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create cron scheduler: {}", e))?;

        let mut registered = 0usize;

        // Register pipeline cron schedules
        let scheduled_pipelines = self.pipeline_store.list_scheduled().await?;
        for (pipeline_id, project_id, name, cron_expr) in &scheduled_pipelines {
            let emitter = Arc::clone(self);
            let project_id = *project_id;
            let pipeline_id = *pipeline_id;
            let name_for_log = name.clone();
            let cron_source = format!("cron:pipeline:{}", pipeline_id);

            match Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
                let emitter = emitter.clone();
                let name = name_for_log.clone();
                let source = cron_source.clone();
                Box::pin(async move {
                    match emitter
                        .event_store
                        .emit(
                            project_id,
                            EventType::Cron,
                            &source,
                            serde_json::json!({ "pipeline_id": pipeline_id }),
                        )
                        .await
                    {
                        Ok(event_id) => {
                            tracing::debug!(
                                pipeline = %name,
                                event_id = %event_id,
                                "Emitted cron event for pipeline"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                pipeline = %name,
                                error = %e,
                                "Failed to emit cron event for pipeline"
                            );
                        }
                    }
                })
            }) {
                Ok(job) => {
                    sched
                        .add(job)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to add pipeline cron job: {}", e))?;
                    registered += 1;
                    tracing::info!(
                        pipeline = %name,
                        cron = %cron_expr,
                        "Registered pipeline cron schedule"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline = %name,
                        cron = %cron_expr,
                        error = %e,
                        "Invalid cron expression, skipping pipeline"
                    );
                }
            }
        }

        // Register UDF job cron schedules
        if let Some(ref registry) = self.udf_registry {
            let scheduled_jobs = registry.scheduled_jobs();
            for (project_id, udf_name, cron_expr) in &scheduled_jobs {
                let emitter = Arc::clone(self);
                let project_id = *project_id;
                let udf_name_for_closure = udf_name.clone();
                let cron_source = format!("cron:udf_job:{}", udf_name);

                match Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
                    let emitter = emitter.clone();
                    let udf_name = udf_name_for_closure.clone();
                    let source = cron_source.clone();
                    Box::pin(async move {
                        match emitter
                            .event_store
                            .emit(
                                project_id,
                                EventType::Cron,
                                &source,
                                serde_json::json!({ "udf_name": udf_name }),
                            )
                            .await
                        {
                            Ok(event_id) => {
                                tracing::debug!(
                                    udf = %udf_name,
                                    event_id = %event_id,
                                    "Emitted cron event for UDF job"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    udf = %udf_name,
                                    error = %e,
                                    "Failed to emit cron event for UDF job"
                                );
                            }
                        }
                    })
                }) {
                    Ok(job) => {
                        sched
                            .add(job)
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to add UDF job cron: {}", e)
                            })?;
                        registered += 1;
                        tracing::info!(
                            udf = %udf_name,
                            cron = %cron_expr,
                            "Registered UDF job cron schedule"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            udf = %udf_name,
                            cron = %cron_expr,
                            error = %e,
                            "Invalid cron expression, skipping UDF job"
                        );
                    }
                }
            }
        }

        if registered == 0 {
            tracing::info!("No cron schedules to register");
            return Ok(());
        }

        sched
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start cron scheduler: {}", e))?;

        tracing::info!(registered = registered, "Cron scheduler started");

        *self.scheduler.lock().await = Some(sched);
        Ok(())
    }
}
