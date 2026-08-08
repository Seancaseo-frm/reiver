//! Global blockchain sync daemon.
//!
//! A background worker that polls `blockchain_global_sources` and syncs each
//! enabled chain to a shared R2 prefix.  All projects that reference a
//! blockchain source read from the same Parquet files — no data duplication.
//!
//! # Reorg handling
//!
//! On every cycle the daemon checks the last `confirmation_depth` block hashes
//! against the full node.  If a mismatch is found (a reorg), data from the
//! fork point forward is re-fetched and merged into the tip partition using
//! the copy-on-write (merge-on-write) path.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;
use tracing::{info, warn, error};
use uuid::Uuid;

use crate::warehouse::connectors::blockchain::bitcoin::{BitcoinConfig, BitcoinConnector};
use crate::warehouse::connectors::blockchain::ethereum::{EthereumConfig, EthereumConnector};
use crate::warehouse::connectors::blockchain::rpc::BitcoinRpcClient;
use crate::warehouse::connectors::blockchain::{BlockchainConnector as EvmRpcClient, eth_schema, schema};
use crate::warehouse::indexes::PartitionManager;
use crate::warehouse::parquet::WriteOptions;
use crate::warehouse::parquet_stats::write_parquet_with_stats;
use crate::warehouse::storage::clickhouse::ClickHouseStorage;
use crate::warehouse::storage::r2::R2Storage;
use crate::warehouse::indexes::persistence::{save_file_skip_index, delete_file_skip_indexes};
use crate::warehouse::indexes::skip_index::FileSkipIndex;
use crate::warehouse::sync::job_worker::extract_indexable_values;
use crate::warehouse::sync::merge::{merge_batches_by_pk, read_parquet_bytes};
use crate::warehouse::sync::sync_executor::split_batches_by_size;

const TARGET_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024;

/// Upper bound on blocks fetched per sync cycle.  Kept small because free
/// public RPC endpoints impose strict rate limits; fetching too many blocks
/// in one call will either hang or get throttled.
const MAX_BLOCKS_PER_CYCLE: u64 = 100;

/// Build a ClickHouse buffer table name for a blockchain chain+table pair.
/// Example: `blockchain_buffer_bitcoin_blocks`
pub fn buffer_table_name(chain: &str, table: &str) -> String {
    format!("blockchain_buffer_{}_{}", chain, table)
}

/// Row from `blockchain_global_sources`.
#[derive(Debug)]
struct GlobalSource {
    id: Uuid,
    chain: String,
    node_config: serde_json::Value,
    r2_prefix: String,
    last_synced_height: i64,
    /// Loaded from DB for checkpoint writes; not read during sync logic.
    #[allow(dead_code)]
    last_synced_hash: Option<String>,
    tip_hashes: serde_json::Value,
    confirmation_depth: i32,
    sync_interval: String,
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// Parse a human-readable interval string (e.g. "10s", "1m", "1h") into a `Duration`.
fn parse_sync_interval(s: &str) -> Duration {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix('s') {
        if let Ok(secs) = rest.parse::<u64>() {
            return Duration::from_secs(secs);
        }
    }
    if let Some(rest) = s.strip_suffix('m') {
        if let Ok(mins) = rest.parse::<u64>() {
            return Duration::from_secs(mins * 60);
        }
    }
    if let Some(rest) = s.strip_suffix('h') {
        if let Ok(hours) = rest.parse::<u64>() {
            return Duration::from_secs(hours * 3600);
        }
    }
    Duration::from_secs(60) // fallback to 1 minute
}

/// The background daemon.
pub struct BlockchainSyncDaemon {
    db: PgPool,
    r2_storage: Arc<R2Storage>,
    ch_storage: Arc<ClickHouseStorage>,
    /// Reserved for future use (e.g. partition index updates after sync).
    #[allow(dead_code)]
    partition_manager: Arc<PartitionManager>,
    /// Cached Bitcoin connectors keyed by chain name, storing (config_hash, connector).
    /// Re-uses the underlying `reqwest::Client` connection pool across cycles.
    connector_cache: Mutex<HashMap<String, (u64, Arc<BitcoinConnector>)>>,
    /// Cached Ethereum connectors keyed by chain name, storing (config_hash, connector).
    eth_connector_cache: Mutex<HashMap<String, (u64, Arc<EthereumConnector>)>>,
}

impl BlockchainSyncDaemon {
    pub fn new(
        db: PgPool,
        r2_storage: Arc<R2Storage>,
        ch_storage: Arc<ClickHouseStorage>,
        partition_manager: Arc<PartitionManager>,
    ) -> Self {
        Self {
            db,
            r2_storage,
            ch_storage,
            partition_manager,
            connector_cache: Mutex::new(HashMap::new()),
            eth_connector_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Ensure ClickHouse buffer tables exist for all blockchain tables.
    /// Called once per sync cycle start; the DDL is idempotent.
    async fn ensure_buffer_tables(&self, chain: &str) -> Result<()> {
        match chain {
            "bitcoin" => {
                for table_name in schema::ALL_TABLES {
                    let ch_table = buffer_table_name(chain, table_name);
                    let table_schema = schema::schema_for_table(table_name)
                        .ok_or_else(|| anyhow::anyhow!("Unknown bitcoin table: {}", table_name))?;
                    let pk = schema::primary_key_for_table(table_name);
                    let order_by: Vec<&str> = pk.iter().map(|s| s.as_str()).collect();
                    self.ch_storage
                        .ensure_buffer_table(&ch_table, &table_schema, &order_by)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to ensure buffer table {}: {}", ch_table, e))?;
                }
            }
            "ethereum" => {
                for table_name in eth_schema::ALL_TABLES {
                    let ch_table = buffer_table_name(chain, table_name);
                    let table_schema = eth_schema::schema_for_table(table_name)
                        .ok_or_else(|| anyhow::anyhow!("Unknown ethereum table: {}", table_name))?;
                    let pk = eth_schema::primary_key_for_table(table_name);
                    let order_by: Vec<&str> = pk.iter().map(|s| s.as_str()).collect();
                    self.ch_storage
                        .ensure_buffer_table(&ch_table, &table_schema, &order_by)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to ensure buffer table {}: {}", ch_table, e))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Run the daemon loop.  Polls every 30 seconds, checks each enabled
    /// global source, and syncs if the interval has elapsed.
    pub async fn run(&self, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        info!("Blockchain sync daemon started");

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Blockchain sync daemon shutdown requested");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if let Err(e) = self.tick().await {
                        error!(error = %e, "Blockchain sync daemon tick failed");
                    }
                }
            }
        }
        Ok(())
    }

    /// Single tick: load enabled global sources and sync any that are due.
    async fn tick(&self) -> Result<()> {
        let sources = self.load_enabled_sources().await?;
        for source in sources {
            if let Err(e) = self.sync_chain(&source).await {
                error!(
                    chain = %source.chain,
                    error = %e,
                    "Failed to sync blockchain"
                );
            }
        }
        Ok(())
    }

    async fn load_enabled_sources(&self) -> Result<Vec<GlobalSource>> {
        let rows = sqlx::query(
            "SELECT id, chain, node_config, r2_prefix, last_synced_height,
                    last_synced_hash, tip_hashes, confirmation_depth, sync_interval,
                    updated_at
             FROM blockchain_global_sources
             WHERE enabled = true"
        )
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| GlobalSource {
                id: r.get("id"),
                chain: r.get("chain"),
                node_config: r.get("node_config"),
                r2_prefix: r.get("r2_prefix"),
                last_synced_height: r.get("last_synced_height"),
                last_synced_hash: r.get("last_synced_hash"),
                tip_hashes: r.get("tip_hashes"),
                confirmation_depth: r.get("confirmation_depth"),
                sync_interval: r.get("sync_interval"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    /// Derive a deterministic advisory-lock key from a chain name.
    /// The key space is namespaced with a fixed prefix to avoid collisions
    /// with other advisory locks used elsewhere in the application.
    fn advisory_lock_key(chain: &str) -> i64 {
        let mut hasher = DefaultHasher::new();
        "blockchain_sync::".hash(&mut hasher);
        chain.hash(&mut hasher);
        hasher.finish() as i64
    }

    /// Sync a single blockchain source, guarded by an advisory lock so only
    /// one daemon instance processes a given chain at a time.
    ///
    /// Uses a dedicated connection (not the pool) for both acquire and release,
    /// because PostgreSQL session-level advisory locks are bound to the
    /// specific connection that acquired them.
    #[tracing::instrument(skip(self, source), fields(chain = %source.chain), err)]
    async fn sync_chain(&self, source: &GlobalSource) -> Result<()> {
        let lock_key = Self::advisory_lock_key(&source.chain);
        let mut conn = self.db.acquire().await?;

        // Non-blocking try: returns false if another instance holds the lock.
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock($1)"
        )
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await?;

        if !acquired {
            info!(
                chain = %source.chain,
                "Another instance is syncing this chain, skipping"
            );
            return Ok(());
        }

        // Wrap the sync in a timeout so a hung RPC call can never block
        // the daemon loop (and hold the advisory lock) indefinitely.
        let result = match tokio::time::timeout(
            Duration::from_secs(300),
            self.sync_chain_inner(source),
        ).await {
            Ok(inner) => inner,
            Err(_) => {
                warn!(chain = %source.chain, "sync_chain_inner timed out after 300s");
                Err(anyhow::anyhow!("sync timed out"))
            }
        };

        // Always release the lock on the same connection, even on error.
        let _: bool = sqlx::query_scalar(
            "SELECT pg_advisory_unlock($1)"
        )
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(false);

        result
    }

    /// Actual sync dispatch (called under advisory lock).
    ///
    /// Respects the per-chain `sync_interval` — skips the sync if not
    /// enough time has elapsed since the last successful run.
    #[tracing::instrument(skip(self, source), fields(chain = %source.chain), err)]
    async fn sync_chain_inner(&self, source: &GlobalSource) -> Result<()> {
        let interval = parse_sync_interval(&source.sync_interval);
        let elapsed = Utc::now()
            .signed_duration_since(source.updated_at)
            .to_std()
            .unwrap_or(Duration::ZERO);

        if elapsed < interval {
            return Ok(());
        }

        match source.chain.as_str() {
            "bitcoin" => self.sync_bitcoin(source).await,
            "ethereum" => self.sync_ethereum(source).await,
            other => {
                warn!(chain = other, "Unsupported chain, skipping");
                Ok(())
            }
        }
    }

    // ── Bitcoin sync ─────────────────────────────────────────────────

    #[tracing::instrument(skip(self, source), fields(chain = %source.chain, from_height = source.last_synced_height), err)]
    async fn sync_bitcoin(&self, source: &GlobalSource) -> Result<()> {
        self.ensure_buffer_tables("bitcoin").await?;

        // Reuse cached connector if the config hasn't changed, avoiding a
        // fresh TCP+TLS handshake on every 30-second cycle.
        let config_hash = {
            let mut hasher = DefaultHasher::new();
            source.node_config.to_string().hash(&mut hasher);
            hasher.finish()
        };

        let connector = {
            let mut cache = self.connector_cache.lock().await;
            match cache.get(&source.chain) {
                Some((h, c)) if *h == config_hash => Arc::clone(c),
                _ => {
                    let config: BitcoinConfig =
                        serde_json::from_value(source.node_config.clone())
                            .map_err(|e| anyhow::anyhow!("Invalid Bitcoin node config: {}", e))?;
                    let c = Arc::new(BitcoinConnector::new(config));
                    cache.insert(source.chain.clone(), (config_hash, Arc::clone(&c)));
                    c
                }
            }
        };

        let rpc = connector.rpc();

        let current_tip = rpc.get_block_count().await
            .map_err(|e| anyhow::anyhow!("Failed to get block count: {}", e))?;
        let last_synced = source.last_synced_height as u64;

        if current_tip == last_synced && last_synced > 0 {
            return Ok(());
        }

        // ── Step 1: Reorg detection ──────────────────────────────────
        let (fork_point, reorg_detected) =
            self.detect_reorg(rpc, source, last_synced).await?;

        // ── Step 2: Determine fetch range ────────────────────────────
        let from_height = if reorg_detected {
            info!(
                chain = "bitcoin",
                fork_point = fork_point,
                old_tip = last_synced,
                new_tip = current_tip,
                "Reorg detected, re-syncing from fork point"
            );
            fork_point + 1
        } else if last_synced == 0 {
            0
        } else {
            last_synced + 1
        };

        if from_height > current_tip {
            return Ok(());
        }

        // Cap the range to bound memory usage per cycle. On initial sync
        // this prevents fetching hundreds of thousands of blocks at once.
        let effective_tip = current_tip.min(from_height + MAX_BLOCKS_PER_CYCLE - 1);

        // ── Step 3: Fetch blocks ─────────────────────────────────────
        let (blocks, txs, inputs, outputs) =
            connector.fetch_block_range(from_height, effective_tip).await
                .map_err(|e| anyhow::anyhow!("Failed to fetch block range: {}", e))?;

        // ── Step 3b: On reorg, purge stale rows from CH buffers ─────
        if reorg_detected {
            for table_name in schema::ALL_TABLES {
                let ch_table = buffer_table_name("bitcoin", table_name);
                self.ch_storage
                    .delete_from_raw(&ch_table, "block_height", from_height as i64)
                    .await
                    .map_err(|e| anyhow::anyhow!("Buffer reorg delete from {} failed: {}", ch_table, e))?;
            }
        }

        // ── Step 4: Write or merge (tables uploaded concurrently) ────
        let tables_data: Vec<(&str, Vec<RecordBatch>, Vec<String>)> = vec![
            (schema::TABLE_BLOCKS, blocks, schema::primary_key_for_table(schema::TABLE_BLOCKS)),
            (schema::TABLE_TRANSACTIONS, txs, schema::primary_key_for_table(schema::TABLE_TRANSACTIONS)),
            (schema::TABLE_INPUTS, inputs, schema::primary_key_for_table(schema::TABLE_INPUTS)),
            (schema::TABLE_OUTPUTS, outputs, schema::primary_key_for_table(schema::TABLE_OUTPUTS)),
        ];

        let upload_futures: Vec<_> = tables_data
            .iter()
            .filter(|(_, batches, _)| !batches.is_empty())
            .map(|(table_name, new_batches, pk_columns)| async move {
                if reorg_detected {
                    self.merge_on_write_table(
                        source, table_name, new_batches, pk_columns, from_height, effective_tip,
                    )
                    .await
                } else {
                    self.append_table(source, table_name, new_batches, from_height, effective_tip)
                        .await
                }
            })
            .collect();

        futures::future::try_join_all(upload_futures).await?;

        // ── Step 5: Update checkpoint ────────────────────────────────
        let new_tip_hashes = self.build_tip_hashes(
            rpc, effective_tip, source.confirmation_depth as u64,
        ).await?;

        let tip_hash = rpc.get_block_hash(effective_tip).await
            .map_err(|e| anyhow::anyhow!("Failed to get tip hash: {}", e))?;

        sqlx::query(
            "UPDATE blockchain_global_sources
             SET last_synced_height = $1,
                 last_synced_hash = $2,
                 tip_hashes = $3,
                 updated_at = NOW()
             WHERE id = $4"
        )
        .bind(effective_tip as i64)
        .bind(&tip_hash)
        .bind(&new_tip_hashes)
        .bind(source.id)
        .execute(&self.db)
        .await?;

        info!(
            chain = "bitcoin",
            from = from_height,
            to = effective_tip,
            reorg = reorg_detected,
            "Bitcoin sync cycle complete"
        );

        Ok(())
    }

    // ── Ethereum sync ────────────────────────────────────────────────

    #[tracing::instrument(skip(self, source), fields(chain = %source.chain, from_height = source.last_synced_height), err)]
    async fn sync_ethereum(&self, source: &GlobalSource) -> Result<()> {
        self.ensure_buffer_tables("ethereum").await?;

        let config_hash = {
            let mut hasher = DefaultHasher::new();
            source.node_config.to_string().hash(&mut hasher);
            hasher.finish()
        };

        let connector = {
            let mut cache = self.eth_connector_cache.lock().await;
            match cache.get(&source.chain) {
                Some((h, c)) if *h == config_hash => Arc::clone(c),
                _ => {
                    let config: EthereumConfig =
                        serde_json::from_value(source.node_config.clone())
                            .map_err(|e| anyhow::anyhow!("Invalid Ethereum node config: {}", e))?;
                    let c = Arc::new(EthereumConnector::new(config));
                    cache.insert(source.chain.clone(), (config_hash, Arc::clone(&c)));
                    c
                }
            }
        };

        let rpc = connector.rpc();

        let current_tip = rpc.get_block_number().await
            .map_err(|e| anyhow::anyhow!("Failed to get block number: {}", e))?;
        let last_synced = source.last_synced_height as u64;

        if current_tip == last_synced && last_synced > 0 {
            return Ok(());
        }

        // ── Step 1: Reorg detection ──────────────────────────────────
        let (fork_point, reorg_detected) =
            self.detect_reorg_evm(rpc, source, last_synced).await?;

        // ── Step 2: Determine fetch range ────────────────────────────
        let from_height = if reorg_detected {
            info!(
                chain = "ethereum",
                fork_point = fork_point,
                old_tip = last_synced,
                new_tip = current_tip,
                "Reorg detected, re-syncing from fork point"
            );
            fork_point + 1
        } else if last_synced == 0 {
            0
        } else {
            last_synced + 1
        };

        if from_height > current_tip {
            return Ok(());
        }

        let effective_tip = current_tip.min(from_height + MAX_BLOCKS_PER_CYCLE - 1);

        // ── Step 3: Fetch blocks ─────────────────────────────────────
        let (blocks, txs, logs) =
            connector.fetch_block_range(from_height, effective_tip).await
                .map_err(|e| anyhow::anyhow!("Failed to fetch block range: {}", e))?;

        // ── Step 3b: On reorg, purge stale rows from CH buffers ─────
        if reorg_detected {
            for table_name in eth_schema::ALL_TABLES {
                let ch_table = buffer_table_name("ethereum", table_name);
                self.ch_storage
                    .delete_from_raw(&ch_table, "block_number", from_height as i64)
                    .await
                    .map_err(|e| anyhow::anyhow!("Buffer reorg delete from {} failed: {}", ch_table, e))?;
            }
        }

        // ── Step 4: Write or merge (tables uploaded concurrently) ────
        let tables_data: Vec<(&str, Vec<RecordBatch>, Vec<String>)> = vec![
            (eth_schema::TABLE_BLOCKS, blocks, eth_schema::primary_key_for_table(eth_schema::TABLE_BLOCKS)),
            (eth_schema::TABLE_TRANSACTIONS, txs, eth_schema::primary_key_for_table(eth_schema::TABLE_TRANSACTIONS)),
            (eth_schema::TABLE_LOGS, logs, eth_schema::primary_key_for_table(eth_schema::TABLE_LOGS)),
        ];

        let upload_futures: Vec<_> = tables_data
            .iter()
            .filter(|(_, batches, _)| !batches.is_empty())
            .map(|(table_name, new_batches, pk_columns)| async move {
                if reorg_detected {
                    self.merge_on_write_table(
                        source, table_name, new_batches, pk_columns, from_height, effective_tip,
                    )
                    .await
                } else {
                    self.append_table(source, table_name, new_batches, from_height, effective_tip)
                        .await
                }
            })
            .collect();

        futures::future::try_join_all(upload_futures).await?;

        // ── Step 5: Update checkpoint ────────────────────────────────
        let new_tip_hashes = self.build_tip_hashes_evm(
            rpc, effective_tip, source.confirmation_depth as u64,
        ).await?;

        let tip_hash = get_block_hash_evm(rpc, effective_tip).await?;

        sqlx::query(
            "UPDATE blockchain_global_sources
             SET last_synced_height = $1,
                 last_synced_hash = $2,
                 tip_hashes = $3,
                 updated_at = NOW()
             WHERE id = $4"
        )
        .bind(effective_tip as i64)
        .bind(&tip_hash)
        .bind(&new_tip_hashes)
        .bind(source.id)
        .execute(&self.db)
        .await?;

        info!(
            chain = "ethereum",
            from = from_height,
            to = effective_tip,
            reorg = reorg_detected,
            "Ethereum sync cycle complete"
        );

        Ok(())
    }

    // ── Reorg detection ──────────────────────────────────────────────

    async fn detect_reorg(
        &self,
        rpc: &BitcoinRpcClient,
        source: &GlobalSource,
        last_synced: u64,
    ) -> Result<(u64, bool)> {
        if last_synced == 0 {
            return Ok((0, false));
        }

        let depth = source.confirmation_depth.max(1) as u64;
        let check_from = last_synced.saturating_sub(depth - 1);
        let tip_hashes = &source.tip_hashes;

        // Collect heights that have stored hashes to check.
        let heights_to_check: Vec<u64> = (check_from..=last_synced)
            .filter(|h| {
                tip_hashes
                    .get(&h.to_string())
                    .and_then(|v| v.as_str())
                    .is_some()
            })
            .collect();

        if heights_to_check.is_empty() {
            return Ok((0, false));
        }

        // Fetch all hashes concurrently (typically ~6 calls).
        let hash_futures: Vec<_> = heights_to_check
            .iter()
            .map(|&h| async move {
                let hash = rpc
                    .get_block_hash(h)
                    .await
                    .map_err(|e| anyhow::anyhow!("Reorg check failed at height {}: {}", h, e))?;
                Ok::<_, anyhow::Error>((h, hash))
            })
            .collect();

        let results = futures::future::try_join_all(hash_futures).await?;

        // Compare in height order (results preserve order from heights_to_check).
        for (height, node_hash) in results {
            let stored = match tip_hashes
                .get(&height.to_string())
                .and_then(|v| v.as_str())
            {
                Some(s) => s,
                None => {
                    warn!(height = height, "Missing stored hash during reorg check, treating as reorg");
                    return Ok((height.saturating_sub(1), true));
                }
            };
            if node_hash != stored {
                return Ok((height.saturating_sub(1), true));
            }
        }

        Ok((0, false))
    }

    async fn build_tip_hashes(
        &self,
        rpc: &BitcoinRpcClient,
        tip: u64,
        depth: u64,
    ) -> Result<serde_json::Value> {
        let depth = depth.max(1);
        let start = tip.saturating_sub(depth - 1);

        // Fetch all hashes concurrently (typically ~6 calls).
        let hash_futures: Vec<_> = (start..=tip)
            .map(|h| async move {
                let hash = rpc
                    .get_block_hash(h)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get hash at {}: {}", h, e))?;
                Ok::<_, anyhow::Error>((h, hash))
            })
            .collect();

        let results = futures::future::try_join_all(hash_futures).await?;

        let mut hashes = serde_json::Map::new();
        for (h, hash) in results {
            hashes.insert(h.to_string(), serde_json::Value::String(hash));
        }
        Ok(serde_json::Value::Object(hashes))
    }

    // ── EVM reorg detection ──────────────────────────────────────────

    /// Like `detect_reorg` but for EVM chains — uses `BlockchainConnector`
    /// (generic EVM RPC) to fetch block hashes via `get_block`.
    async fn detect_reorg_evm(
        &self,
        rpc: &EvmRpcClient,
        source: &GlobalSource,
        last_synced: u64,
    ) -> Result<(u64, bool)> {
        if last_synced == 0 {
            return Ok((0, false));
        }

        let depth = source.confirmation_depth.max(1) as u64;
        let check_from = last_synced.saturating_sub(depth - 1);
        let tip_hashes = &source.tip_hashes;

        let heights_to_check: Vec<u64> = (check_from..=last_synced)
            .filter(|h| {
                tip_hashes
                    .get(&h.to_string())
                    .and_then(|v| v.as_str())
                    .is_some()
            })
            .collect();

        if heights_to_check.is_empty() {
            return Ok((0, false));
        }

        let hash_futures: Vec<_> = heights_to_check
            .iter()
            .map(|&h| async move {
                let hash = get_block_hash_evm(rpc, h).await?;
                Ok::<_, anyhow::Error>((h, hash))
            })
            .collect();

        let results = futures::future::try_join_all(hash_futures).await?;

        for (height, node_hash) in results {
            let stored = match tip_hashes
                .get(&height.to_string())
                .and_then(|v| v.as_str())
            {
                Some(s) => s,
                None => {
                    warn!(height = height, "Missing stored hash during ETH reorg check, treating as reorg");
                    return Ok((height.saturating_sub(1), true));
                }
            };
            if node_hash != stored {
                return Ok((height.saturating_sub(1), true));
            }
        }

        Ok((0, false))
    }

    /// Like `build_tip_hashes` but for EVM chains.
    async fn build_tip_hashes_evm(
        &self,
        rpc: &EvmRpcClient,
        tip: u64,
        depth: u64,
    ) -> Result<serde_json::Value> {
        let depth = depth.max(1);
        let start = tip.saturating_sub(depth - 1);

        let hash_futures: Vec<_> = (start..=tip)
            .map(|h| async move {
                let hash = get_block_hash_evm(rpc, h).await?;
                Ok::<_, anyhow::Error>((h, hash))
            })
            .collect();

        let results = futures::future::try_join_all(hash_futures).await?;

        let mut hashes = serde_json::Map::new();
        for (h, hash) in results {
            hashes.insert(h.to_string(), serde_json::Value::String(hash));
        }
        Ok(serde_json::Value::Object(hashes))
    }

    // ── Write paths ──────────────────────────────────────────────────

    /// Append new data as fresh Parquet files (no reorg, normal path).
    ///
    /// Filenames are deterministic (based on height range + sequence number),
    /// making uploads idempotent.  If the process crashes after uploading
    /// files but before advancing the checkpoint, the next sync cycle will
    /// overwrite the same keys — no duplicate data.
    /// Insert batches into the ClickHouse buffer table for this chain+table.
    async fn append_table(
        &self,
        source: &GlobalSource,
        table_name: &str,
        batches: &[RecordBatch],
        _from_height: u64,
        _to_height: u64,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let ch_table = buffer_table_name(&source.chain, table_name);
        for batch in batches {
            self.ch_storage
                .insert_batch_raw(&ch_table, batch)
                .await
                .map_err(|e| anyhow::anyhow!("Buffer insert into {} failed: {}", ch_table, e))?;
        }

        Ok(())
    }

    /// Parse the block-height range from a filename like `h100-200_{uuid}.parquet`.
    /// Returns `None` for legacy files without a height prefix (they are always
    /// included so they can be merged/migrated).
    fn parse_height_range(key: &str) -> Option<(u64, u64)> {
        let filename = key.rsplit('/').next()?;
        if !filename.starts_with('h') {
            return None;
        }
        let rest = &filename[1..];
        let dash = rest.find('-')?;
        let underscore = rest.find('_')?;
        let from: u64 = rest[..dash].parse().ok()?;
        let to: u64 = rest[dash + 1..underscore].parse().ok()?;
        Some((from, to))
    }

    /// Returns `true` if the file might contain blocks in `[reorg_from, reorg_to]`.
    ///
    /// Legacy files (no height in name) are always considered overlapping
    /// so they get merged and rewritten with the new naming convention.
    fn file_overlaps_range(key: &str, reorg_from: u64, reorg_to: u64) -> bool {
        match Self::parse_height_range(key) {
            Some((file_from, file_to)) => file_from <= reorg_to && file_to >= reorg_from,
            None => true, // legacy file — include it to be safe
        }
    }

    /// Merge-on-write: download only the existing files whose height range
    /// overlaps the reorg window, merge by PK, re-upload, and clean up stale
    /// copies.
    async fn merge_on_write_table(
        &self,
        source: &GlobalSource,
        table_name: &str,
        new_batches: &[RecordBatch],
        pk_columns: &[String],
        from_height: u64,
        to_height: u64,
    ) -> Result<()> {
        let table_prefix = format!("{}/{}", source.r2_prefix, table_name);

        // List existing files and filter to those overlapping the reorg range.
        let all_objects = self.r2_storage
            .list_objects(&table_prefix)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list R2 objects: {}", e))?;

        let overlapping: Vec<_> = all_objects
            .iter()
            .filter(|obj| Self::file_overlaps_range(&obj.key, from_height, to_height))
            .collect();

        // Track the actual height range across all overlapping files and new
        // data so the output filename accurately reflects the content.
        let mut actual_min = from_height;
        let mut actual_max = to_height;

        for obj in &overlapping {
            if let Some((file_from, file_to)) = Self::parse_height_range(&obj.key) {
                actual_min = actual_min.min(file_from);
                actual_max = actual_max.max(file_to);
            }
        }

        // Download overlapping Parquet files concurrently.
        let old_keys: Vec<String> = overlapping.iter().map(|obj| obj.key.clone()).collect();

        let download_results: Vec<Result<_, anyhow::Error>> =
            stream::iter(old_keys.iter().cloned().map(|key| async move {
                let data = self
                    .r2_storage
                    .download(&key)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to download {}: {}", key, e))?;
                Ok::<_, anyhow::Error>((key, data))
            }))
            .buffer_unordered(4)
            .collect()
            .await;

        let mut existing_batches: Vec<RecordBatch> = Vec::new();
        for result in download_results {
            let (key, data) = result?;
            match read_parquet_bytes(&data) {
                Ok(batches) => existing_batches.extend(batches),
                Err(e) => {
                    warn!(
                        key = key.as_str(),
                        error = %e,
                        "Skipping corrupted R2 object during merge (will be replaced)"
                    );
                }
            }
        }

        // Merge new data into existing by PK.
        let merged = merge_batches_by_pk(&existing_batches, new_batches, pk_columns)?;

        if merged.is_empty() {
            return Ok(());
        }

        // Remove skip indexes for files being replaced.
        if !old_keys.is_empty() {
            if let Err(e) = delete_file_skip_indexes(
                &self.db, source.id, table_name, &old_keys,
            ).await {
                warn!(error = %e, "Failed to delete stale skip indexes during merge (non-fatal)");
            }
        }

        // Write merged output with deterministic naming for idempotency.
        // The filename uses the actual height range (union of old files and new
        // data) so that future overlap detection is correct.
        let chunks = split_batches_by_size(&merged, TARGET_FILE_SIZE_BYTES);
        let mut new_keys: Vec<String> = Vec::new();

        for (seq, chunk) in chunks.iter().enumerate() {
            let schema_arc = chunk[0].schema();
            let (parquet_bytes, stats) = write_parquet_with_stats(schema_arc, chunk, WriteOptions::default())
                .map_err(|e| anyhow::anyhow!("Failed to write merged Parquet: {}", e))?;

            let key = format!(
                "{}/h{}-{}_{}.parquet",
                table_prefix, actual_min, actual_max, seq,
            );

            self.r2_storage
                .upload_parquet_with_stats(&key, parquet_bytes, &stats)
                .await
                .map_err(|e| anyhow::anyhow!("R2 upload failed: {}", e))?;

            let column_values = extract_indexable_values(chunk);
            if !column_values.is_empty() {
                if let Ok(index) = FileSkipIndex::build(&key, column_values) {
                    let row_count: u64 = chunk.iter().map(|b| b.num_rows() as u64).sum();
                    if let Err(e) = save_file_skip_index(
                        &self.db, source.id, table_name, "default",
                        &index, row_count, None,
                    ).await {
                        warn!(file = %key, error = %e, "Failed to save skip index for merged blockchain file");
                    }
                }
            }

            new_keys.push(key);
        }

        // Best-effort cleanup of old overlapping files and their stats sidecars.
        for old_key in &old_keys {
            if new_keys.contains(old_key) {
                continue;
            }
            if let Err(e) = self.r2_storage.delete_with_stats(old_key).await {
                warn!(
                    key = old_key.as_str(),
                    error = %e,
                    "Failed to delete old R2 object (non-fatal)"
                );
            }
        }

        Ok(())
    }
}

/// Extract block hash from an EVM block header (without full tx bodies).
async fn get_block_hash_evm(rpc: &EvmRpcClient, height: u64) -> Result<String> {
    let header = rpc
        .get_block_header(height)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get EVM block header at {}: {}", height, e))?;
    header
        .get("hash")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Block at height {} missing 'hash' field", height))
}

/// Background worker that periodically flushes confirmed rows from ClickHouse
/// buffer tables to R2 as Parquet files.
///
/// Flush cutoff is `max(block_height) - confirmation_depth` to avoid flushing
/// rows that could still be reorged.
pub async fn blockchain_buffer_flush_worker(
    db: PgPool,
    ch_storage: Arc<ClickHouseStorage>,
    r2_storage: Arc<R2Storage>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    info!("Blockchain buffer flush worker started");

    let bucket = std::env::var("R2_BUCKET").unwrap_or_else(|_| "warehouse".to_string());
    let s3_collection = format!("r2_{}", bucket.replace('-', "_"));

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Blockchain buffer flush worker shutdown requested");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                if let Err(e) = flush_tick(&db, &ch_storage, &r2_storage, &s3_collection).await {
                    error!(error = %e, "Blockchain buffer flush tick failed");
                }
            }
        }
    }
    Ok(())
}

/// Height column name differs between Bitcoin (`block_height`) and EVM chains (`block_number`).
fn height_column_for_chain(chain: &str) -> &'static str {
    match chain {
        "bitcoin" => "block_height",
        _ => "block_number",
    }
}

/// All table names for a given chain.
fn all_tables_for_chain(chain: &str) -> &'static [&'static str] {
    match chain {
        "bitcoin" => schema::ALL_TABLES,
        "ethereum" => eth_schema::ALL_TABLES,
        _ => &[],
    }
}

async fn flush_tick(
    db: &PgPool,
    ch_storage: &ClickHouseStorage,
    r2_storage: &R2Storage,
    s3_collection: &str,
) -> Result<()> {
    let sources = sqlx::query(
        "SELECT chain, r2_prefix, confirmation_depth
         FROM blockchain_global_sources
         WHERE enabled = true",
    )
    .fetch_all(db)
    .await?;

    for row in &sources {
        let chain: String = row.get("chain");
        let r2_prefix: String = row.get("r2_prefix");
        let confirmation_depth: i32 = row.get("confirmation_depth");

        let height_col = height_column_for_chain(&chain);
        let tables = all_tables_for_chain(&chain);

        for table_name in tables {
            if let Err(e) = flush_single_table(
                ch_storage,
                r2_storage,
                db,
                s3_collection,
                &chain,
                table_name,
                &r2_prefix,
                height_col,
                confirmation_depth as u64,
            )
            .await
            {
                warn!(
                    chain = %chain,
                    table = %table_name,
                    error = %e,
                    "Failed to flush buffer table"
                );
            }
        }
    }

    Ok(())
}

async fn flush_single_table(
    ch_storage: &ClickHouseStorage,
    r2_storage: &R2Storage,
    db: &PgPool,
    s3_collection: &str,
    chain: &str,
    table_name: &str,
    r2_prefix: &str,
    height_col: &str,
    confirmation_depth: u64,
) -> Result<()> {
    let ch_table = buffer_table_name(chain, table_name);
    let db_name = ch_storage.database();

    // Find the max height in the buffer
    let max_sql = format!(
        "SELECT max(`{}`) FROM `{}`.`{}`",
        height_col, db_name, ch_table
    );
    let max_resp = ch_storage
        .query_text(&max_sql)
        .await
        .map_err(|e| anyhow::anyhow!("Max height query failed: {}", e))?;
    let max_height: i64 = max_resp.trim().parse().unwrap_or(0);

    if max_height == 0 {
        return Ok(());
    }

    let cutoff = (max_height as u64).saturating_sub(confirmation_depth);
    if cutoff == 0 {
        return Ok(());
    }

    // Count rows below cutoff
    let count_sql = format!(
        "SELECT count() FROM `{}`.`{}` WHERE `{}` < {}",
        db_name, ch_table, height_col, cutoff
    );
    let count_resp = ch_storage
        .query_text(&count_sql)
        .await
        .map_err(|e| anyhow::anyhow!("Count query failed: {}", e))?;
    let row_count: u64 = count_resp.trim().parse().unwrap_or(0);

    if row_count == 0 {
        return Ok(());
    }

    // Determine the height range for the filename
    let min_sql = format!(
        "SELECT min(`{}`) FROM `{}`.`{}` WHERE `{}` < {}",
        height_col, db_name, ch_table, height_col, cutoff
    );
    let min_resp = ch_storage
        .query_text(&min_sql)
        .await
        .map_err(|e| anyhow::anyhow!("Min height query failed: {}", e))?;
    let min_height: u64 = min_resp.trim().parse().unwrap_or(0);

    let r2_key = format!(
        "{}/{}/h{}-{}.parquet",
        r2_prefix, table_name, min_height, cutoff.saturating_sub(1)
    );

    let where_clause = format!("WHERE `{}` < {}", height_col, cutoff);

    // Export to R2
    let exported = ch_storage
        .export_raw_to_s3(&ch_table, s3_collection, &r2_key, &where_clause)
        .await
        .map_err(|e| anyhow::anyhow!("Export to R2 failed: {}", e))?;

    if exported == 0 {
        return Ok(());
    }

    // Build skip index from the buffer data before deleting.
    // Query distinct values per indexable string column.
    if let Err(e) = build_and_save_flush_skip_index(
        ch_storage, db, r2_storage, chain, table_name, &ch_table, &r2_key,
        height_col, cutoff, exported,
    )
    .await
    {
        warn!(
            file = %r2_key,
            error = %e,
            "Failed to build skip index for flushed file (non-fatal)"
        );
    }

    // Delete flushed rows from buffer
    ch_storage
        .delete_raw_where(&ch_table, &where_clause)
        .await
        .map_err(|e| anyhow::anyhow!("Buffer delete failed: {}", e))?;

    info!(
        chain = %chain,
        table = %table_name,
        rows = exported,
        r2_key = %r2_key,
        min_height = min_height,
        max_height_excl = cutoff,
        "Flushed buffer to R2"
    );

    Ok(())
}

/// Build a FileSkipIndex for the flushed file by querying distinct values
/// from the ClickHouse buffer table (pre-delete), then persist as a blob to R2.
async fn build_and_save_flush_skip_index(
    ch_storage: &ClickHouseStorage,
    db: &PgPool,
    r2_storage: &R2Storage,
    chain: &str,
    table_name: &str,
    ch_table: &str,
    r2_key: &str,
    height_col: &str,
    cutoff: u64,
    row_count: u64,
) -> Result<()> {
    use crate::warehouse::indexes::skip_index::HierarchicalSkipIndex;
    use crate::warehouse::indexes::persistence::persist_skip_index_blob;

    let table_schema = match chain {
        "bitcoin" => schema::schema_for_table(table_name),
        "ethereum" => eth_schema::schema_for_table(table_name),
        _ => None,
    };

    let table_schema = match table_schema {
        Some(s) => s,
        None => return Ok(()),
    };

    let db_name = ch_storage.database();
    let mut column_values: HashMap<String, Vec<String>> = HashMap::new();

    for col in &table_schema.columns {
        use crate::warehouse::types::ColumnType;
        if col.data_type != ColumnType::String {
            continue;
        }

        let sql = format!(
            "SELECT DISTINCT `{}` FROM `{}`.`{}` WHERE `{}` < {} LIMIT 50000",
            col.name, db_name, ch_table, height_col, cutoff
        );

        let resp = match ch_storage.query_text(&sql).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        let values: Vec<String> = resp
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.trim().to_string())
            .collect();

        if !values.is_empty() && values.len() <= 50000 {
            column_values.insert(col.name.clone(), values);
        }
    }

    if column_values.is_empty() {
        return Ok(());
    }

    let file_index = FileSkipIndex::build(r2_key, column_values)
        .map_err(|e| anyhow::anyhow!("Failed to build skip index: {}", e))?;

    let source_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM blockchain_global_sources WHERE chain = $1 AND enabled = true LIMIT 1",
    )
    .bind(chain)
    .fetch_optional(db)
    .await?;

    if let Some(sid) = source_id {
        let mut hier_index = HierarchicalSkipIndex::new();
        hier_index
            .add_file("default", file_index, row_count)
            .map_err(|e| anyhow::anyhow!("Failed to add file to hierarchical index: {}", e))?;

        persist_skip_index_blob(db, r2_storage, None, sid, table_name, &hier_index)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to persist skip index blob: {}", e))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_source_parsing() {
        let config = serde_json::json!({
            "rpc_url": "http://127.0.0.1:8332",
            "rpc_user": "user",
            "rpc_password": "pass"
        });
        let parsed: Result<BitcoinConfig, _> = serde_json::from_value(config);
        assert!(parsed.is_ok());
        let cfg = parsed.unwrap();
        assert_eq!(cfg.rpc_url, "http://127.0.0.1:8332");
        assert_eq!(cfg.rpc_user.as_deref(), Some("user"));
    }

    #[test]
    fn test_parse_sync_interval() {
        assert_eq!(parse_sync_interval("10s"), Duration::from_secs(10));
        assert_eq!(parse_sync_interval("30s"), Duration::from_secs(30));
        assert_eq!(parse_sync_interval("1m"), Duration::from_secs(60));
        assert_eq!(parse_sync_interval("5m"), Duration::from_secs(300));
        assert_eq!(parse_sync_interval("1h"), Duration::from_secs(3600));
        // Fallback for unparseable input
        assert_eq!(parse_sync_interval("bogus"), Duration::from_secs(60));
    }

    #[test]
    fn test_advisory_lock_keys_are_stable_and_distinct() {
        let btc = BlockchainSyncDaemon::advisory_lock_key("bitcoin");
        let eth = BlockchainSyncDaemon::advisory_lock_key("ethereum");
        assert_ne!(btc, eth);
        // Deterministic: same input always gives same output.
        assert_eq!(btc, BlockchainSyncDaemon::advisory_lock_key("bitcoin"));
    }

    #[test]
    fn test_parse_height_range() {
        assert_eq!(
            BlockchainSyncDaemon::parse_height_range("global/bitcoin/blocks/h100-200_abc.parquet"),
            Some((100, 200))
        );
        assert_eq!(
            BlockchainSyncDaemon::parse_height_range("global/bitcoin/blocks/h0-99999_xyz.parquet"),
            Some((0, 99999))
        );
        // Legacy file without height prefix
        assert_eq!(
            BlockchainSyncDaemon::parse_height_range("global/bitcoin/blocks/0_abc.parquet"),
            None
        );
    }

    #[test]
    fn test_file_overlaps_range() {
        // File h100-200, reorg range 150-250: overlaps
        assert!(BlockchainSyncDaemon::file_overlaps_range(
            "pfx/h100-200_x.parquet", 150, 250
        ));
        // File h100-200, reorg range 201-300: no overlap
        assert!(!BlockchainSyncDaemon::file_overlaps_range(
            "pfx/h100-200_x.parquet", 201, 300
        ));
        // File h100-200, reorg range 50-99: no overlap
        assert!(!BlockchainSyncDaemon::file_overlaps_range(
            "pfx/h100-200_x.parquet", 50, 99
        ));
        // Legacy file always overlaps
        assert!(BlockchainSyncDaemon::file_overlaps_range(
            "pfx/0_abc.parquet", 999, 1000
        ));
    }

    #[test]
    fn test_tip_hashes_format() {
        let mut hashes = serde_json::Map::new();
        hashes.insert("100".to_string(), serde_json::json!("abc123"));
        hashes.insert("101".to_string(), serde_json::json!("def456"));
        let val = serde_json::Value::Object(hashes);

        assert_eq!(
            val.get("100").and_then(|v| v.as_str()),
            Some("abc123")
        );
        assert_eq!(
            val.get("101").and_then(|v| v.as_str()),
            Some("def456")
        );
    }
}
