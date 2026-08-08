use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::kafka::PipelineEventKafkaMessage;

use super::events::{EventStore, PipelineEvent};
use super::executor::PipelineExecutor;
use super::store::PipelineStore;
use super::types::PipelineMode;

pub struct PipelineEventConsumerContext;

impl rdkafka::ClientContext for PipelineEventConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {}
}

impl rdkafka::consumer::ConsumerContext for PipelineEventConsumerContext {}

pub struct PipelineEventConsumerConfig {
    pub kafka_hosts: String,
    pub pipeline_events_topic: String,
    pub client_id: Option<String>,
}

pub struct EventDispatcher {
    consumer: StreamConsumer<PipelineEventConsumerContext>,
    event_store: Arc<EventStore>,
    pipeline_store: Arc<PipelineStore>,
    executor: Arc<PipelineExecutor>,
    shutdown_rx: watch::Receiver<bool>,
    streaming_shutdown_tx: watch::Sender<bool>,
    running_streams: tokio::sync::Mutex<HashSet<Uuid>>,
}

pub struct EventDispatcherHandle {
    shutdown_tx: watch::Sender<bool>,
}

impl EventDispatcherHandle {
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl EventDispatcher {
    pub fn new(
        kafka_config: PipelineEventConsumerConfig,
        event_store: Arc<EventStore>,
        pipeline_store: Arc<PipelineStore>,
        executor: Arc<PipelineExecutor>,
    ) -> Result<(Self, EventDispatcherHandle)> {
        tracing::info!(
            "Creating Kafka consumer for pipeline events topic: {}",
            kafka_config.pipeline_events_topic
        );

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &kafka_config.kafka_hosts)
            .set("group.id", "pond-pipeline-dispatcher")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "30000")
            .set("enable.partition.eof", "false");

        if let Some(ref client_id) = kafka_config.client_id {
            client_config.set("client.id", client_id);
        }

        let consumer: StreamConsumer<PipelineEventConsumerContext> =
            client_config.create_with_context(PipelineEventConsumerContext)?;

        consumer.subscribe(&[&kafka_config.pipeline_events_topic])?;
        tracing::info!(
            "Subscribed to Kafka topic: {}",
            kafka_config.pipeline_events_topic
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (streaming_shutdown_tx, _) = watch::channel(false);

        let dispatcher = Self {
            consumer,
            event_store,
            pipeline_store,
            executor,
            shutdown_rx,
            streaming_shutdown_tx,
            running_streams: tokio::sync::Mutex::new(HashSet::new()),
        };

        let handle = EventDispatcherHandle { shutdown_tx };
        Ok((dispatcher, handle))
    }

    pub async fn run(&mut self) {
        tracing::info!("EventDispatcher started (Kafka consumer)");

        let mut message_stream = self.consumer.stream();

        loop {
            tokio::select! {
                message_opt = message_stream.next() => {
                    let Some(message) = message_opt else { break; };
                    match message {
                        Ok(m) => {
                            match self.process_message(&m).await {
                                Ok(()) => {
                                    if let Err(e) = self.consumer.commit_message(
                                        &m,
                                        rdkafka::consumer::CommitMode::Async,
                                    ) {
                                        tracing::error!("Failed to commit pipeline event offset: {}", e);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to process pipeline event message: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Pipeline event consumer error: {}", e);
                        }
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("EventDispatcher shutting down");
                        let _ = self.streaming_shutdown_tx.send(true);
                        return;
                    }
                }
            }
        }
    }

    async fn process_message<M: Message>(&self, message: &M) -> Result<()> {
        let payload = message
            .payload()
            .ok_or_else(|| anyhow::anyhow!("Empty payload"))?;

        let msg: PipelineEventKafkaMessage = serde_json::from_slice(payload)?;

        let event = PipelineEvent {
            id: msg.event_id,
            project_id: msg.project_id,
            event_type: msg.event_type.clone(),
            source: msg.source.clone(),
            payload: msg.payload.clone(),
            status: "dispatched".to_string(),
            created_at: chrono::Utc::now(),
            dispatched_at: Some(chrono::Utc::now()),
            completed_at: None,
        };

        match self.dispatch_event(&event).await {
            Ok(()) => {
                let _ = self.event_store.complete(event.id).await;
            }
            Err(e) => {
                tracing::error!(
                    event_id = %event.id,
                    event_type = %event.event_type,
                    error = %e,
                    "Failed to dispatch event"
                );
                let _ = self.event_store.fail(event.id, &e.to_string()).await;
            }
        }

        Ok(())
    }

    async fn dispatch_event(&self, event: &PipelineEvent) -> Result<()> {
        let subscriptions = self
            .event_store
            .get_subscriptions_for_event(&event.event_type)
            .await?;

        for sub in subscriptions {
            if !EventStore::matches_filter(&event.payload, &sub.event_filter) {
                continue;
            }

            let pipeline = self
                .pipeline_store
                .load(event.project_id, sub.pipeline_id)
                .await?;

            let Some(pipeline) = pipeline else {
                tracing::warn!(
                    pipeline_id = %sub.pipeline_id,
                    "Subscribed pipeline not found, skipping"
                );
                continue;
            };

            if !pipeline.enabled {
                continue;
            }

            let trigger = format!("event:{}", event.event_type);

            match pipeline.mode() {
                PipelineMode::Batch => {
                    let executor = self.executor.clone();
                    let project_id = event.project_id;
                    let pipeline_id = sub.pipeline_id;
                    let trigger = trigger.clone();
                    tokio::spawn(async move {
                        match executor.run(project_id, pipeline_id, &trigger).await {
                            Ok(run_id) => {
                                tracing::info!(
                                    pipeline_id = %pipeline_id,
                                    run_id = %run_id,
                                    "Event-triggered batch pipeline completed"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    pipeline_id = %pipeline_id,
                                    error = %e,
                                    "Event-triggered batch pipeline failed"
                                );
                            }
                        }
                    });
                }
                PipelineMode::Streaming => {
                    let mut running = self.running_streams.lock().await;
                    if running.contains(&sub.pipeline_id) {
                        tracing::debug!(
                            pipeline_id = %sub.pipeline_id,
                            "Streaming pipeline already running, skipping"
                        );
                        continue;
                    }
                    running.insert(sub.pipeline_id);
                    drop(running);

                    let executor = self.executor.clone();
                    let project_id = event.project_id;
                    let pipeline_id = sub.pipeline_id;
                    let trigger = trigger.clone();
                    let shutdown_rx = self.streaming_shutdown_tx.subscribe();

                    tokio::spawn(async move {
                        match executor
                            .run_streaming(project_id, pipeline_id, &trigger, shutdown_rx)
                            .await
                        {
                            Ok(run_id) => {
                                tracing::info!(
                                    pipeline_id = %pipeline_id,
                                    run_id = %run_id,
                                    "Streaming pipeline completed"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    pipeline_id = %pipeline_id,
                                    error = %e,
                                    "Streaming pipeline failed"
                                );
                            }
                        }
                    });
                }
            }
        }

        Ok(())
    }
}
