//! Batched ClickHouse writes for gateway LLM requests.
//!
//! Receives LlmRequest items on a channel and flushes them in batches to avoid
//! one insert per request under high throughput.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, warn, Instrument};

use crate::clickhouse_db::ClickHousePool;
use crate::llm::LlmRequest;
use crate::llm::LlmSpanProcessor;

const CHANNEL_CAP: usize = 10_000;
const BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL_MS: u64 = 1000;

/// Spawns the buffer flusher task and returns a sender. Callers send prepared
/// `LlmRequest` (with cost already set); the flusher batches and inserts.
pub fn spawn(
    processor: Arc<LlmSpanProcessor>,
    clickhouse: Arc<ClickHousePool>,
) -> mpsc::Sender<LlmRequest> {
    let (tx, rx) = mpsc::channel(CHANNEL_CAP);
    tokio::spawn(async move {
        flusher_loop(rx, processor, clickhouse).await;
    });
    tx
}

async fn flusher_loop(
    mut rx: mpsc::Receiver<LlmRequest>,
    processor: Arc<LlmSpanProcessor>,
    clickhouse: Arc<ClickHousePool>,
) {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let timeout = Duration::from_millis(FLUSH_INTERVAL_MS);
    let mut interval = tokio::time::interval(timeout);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(request) = rx.recv() => {
                batch.push(request);
                if batch.len() >= BATCH_SIZE {
                    let batch_size = batch.len();
                    if let Err(e) = flush_batch(processor.as_ref(), clickhouse.as_ref(), &mut batch)
                        .instrument(tracing::info_span!(
                            "gateway.request_buffer.flush",
                            batch_size
                        ))
                        .await
                    {
                        warn!(error = %e, count = batch.len(), "llm_request_buffer: flush failed");
                    } else {
                        debug!(count = batch.len(), "llm_request_buffer: flushed batch");
                    }
                    batch.clear();
                }
            }
            _ = interval.tick() => {
                if batch.is_empty() {
                    continue;
                }
                let batch_size = batch.len();
                if let Err(e) = flush_batch(processor.as_ref(), clickhouse.as_ref(), &mut batch)
                    .instrument(tracing::info_span!(
                        "gateway.request_buffer.flush",
                        batch_size
                    ))
                    .await
                {
                    warn!(error = %e, count = batch.len(), "llm_request_buffer: periodic flush failed");
                } else {
                    debug!(count = batch.len(), "llm_request_buffer: periodic flush");
                }
                batch.clear();
            }
            else => {
                if !batch.is_empty() {
                    let batch_size = batch.len();
                    if let Err(e) = flush_batch(processor.as_ref(), clickhouse.as_ref(), &mut batch)
                        .instrument(tracing::info_span!(
                            "gateway.request_buffer.flush",
                            batch_size
                        ))
                        .await
                    {
                        error!(error = %e, count = batch.len(), "llm_request_buffer: final flush failed");
                    }
                }
                break;
            }
        }
    }
}

async fn flush_batch(
    processor: &LlmSpanProcessor,
    clickhouse: &ClickHousePool,
    batch: &mut Vec<LlmRequest>,
) -> Result<(), crate::error::AppError> {
    if batch.is_empty() {
        return Ok(());
    }
    let requests = std::mem::take(batch);
    processor
        .insert_llm_requests_batch(&requests, clickhouse)
        .await
}
