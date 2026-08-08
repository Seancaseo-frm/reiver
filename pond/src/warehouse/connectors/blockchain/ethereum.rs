//! Ethereum connector — wraps the generic `BlockchainConnector` for Ethereum
//! (and EVM-compatible) full nodes.
//!
//! Exposes three tables (blocks, transactions, logs) and uses `block_number`
//! as the incremental key for height-based syncing.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::arrow_utils::{opt_string_array, string_array, to_arrow_schema};
use super::eth_schema;
use super::{BlockchainConfig, BlockchainConnector, BlockchainType};
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};

// Cached Arrow schemas -- built once, shared across all `into_record_batch` calls.
static BLOCKS_ARROW_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(to_arrow_schema(&eth_schema::blocks_schema())));
static TXS_ARROW_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(to_arrow_schema(&eth_schema::transactions_schema())));
static LOGS_ARROW_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(to_arrow_schema(&eth_schema::logs_schema())));

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the Ethereum connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumConfig {
    pub rpc_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_timeout() -> u64 {
    30
}
fn default_batch_size() -> u64 {
    50
}
fn default_max_retries() -> u32 {
    3
}

impl EthereumConfig {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            api_key: None,
            timeout_secs: default_timeout(),
            batch_size: default_batch_size(),
            max_retries: default_max_retries(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector
// ═══════════════════════════════════════════════════════════════════════════

/// Controls which tables are parsed when processing raw block JSON.
#[derive(Debug, Clone, Copy)]
struct ParseTables {
    blocks: bool,
    transactions: bool,
    logs: bool,
}

impl ParseTables {
    fn all() -> Self {
        Self {
            blocks: true,
            transactions: true,
            logs: true,
        }
    }

    fn single(name: &str) -> Self {
        Self {
            blocks: name == eth_schema::TABLE_BLOCKS,
            transactions: name == eth_schema::TABLE_TRANSACTIONS,
            logs: name == eth_schema::TABLE_LOGS,
        }
    }
}

/// Ethereum connector that fetches block data from an EVM node.
pub struct EthereumConnector {
    inner: BlockchainConnector,
}

impl std::fmt::Debug for EthereumConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthereumConnector")
            .field("rpc_url", &self.inner.rpc_url())
            .finish()
    }
}

impl EthereumConnector {
    /// Maximum batch size enforced to avoid overwhelming RPC nodes and
    /// exceeding `eth_getLogs` range limits on most providers.
    const MAX_BATCH_SIZE: u64 = 2_000;

    pub fn new(config: EthereumConfig) -> Self {
        let batch_size = config.batch_size.clamp(1, Self::MAX_BATCH_SIZE);
        let bc = BlockchainConfig {
            chain: BlockchainType::Ethereum,
            rpc_url: config.rpc_url,
            api_key: config.api_key,
            timeout: Duration::from_secs(config.timeout_secs),
            max_retries: config.max_retries,
            batch_size,
        };
        let inner = BlockchainConnector::new(bc);
        Self { inner }
    }

    /// Expose the underlying RPC client for use by the global sync daemon.
    pub fn rpc(&self) -> &BlockchainConnector {
        &self.inner
    }

    /// Fetch blocks in the given height range and convert them into
    /// `RecordBatch` vectors keyed by table name.
    pub async fn fetch_block_range(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> ConnectorResult<(Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>)> {
        self.fetch_block_range_filtered(from_height, to_height, ParseTables::all())
            .await
    }

    /// Internal: fetch blocks and only parse the tables indicated by `parse`.
    ///
    /// Blocks within each batch are fetched concurrently (up to 8 in flight),
    /// then sorted by block number for deterministic output.  Logs are fetched
    /// concurrently alongside blocks via `tokio::join!` since they are
    /// independent data sources pushed to separate row builders.
    async fn fetch_block_range_filtered(
        &self,
        from_height: u64,
        to_height: u64,
        parse: ParseTables,
    ) -> ConnectorResult<(Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>)> {
        const MAX_CONCURRENT: usize = 8;

        let mut all_blocks = Vec::new();
        let mut all_txs = Vec::new();
        let mut all_logs = Vec::new();

        let batch_size = self.inner.batch_size();
        let concurrency = (batch_size as usize).min(MAX_CONCURRENT);
        let mut current = from_height;

        while current <= to_height {
            let batch_end = std::cmp::min(current + batch_size - 1, to_height);

            // Launch block fetches and log fetch concurrently -- they are
            // independent and write to separate row builders.
            let needs_blocks = parse.blocks || parse.transactions;
            let blocks_fut = async {
                if !needs_blocks {
                    return Ok::<_, ConnectorError>(vec![]);
                }
                let results: Vec<ConnectorResult<(u64, JsonValue)>> =
                    stream::iter((current..=batch_end).map(|height| async move {
                        let block = self.inner.get_block(height).await?;
                        Ok((height, block))
                    }))
                    .buffer_unordered(concurrency)
                    .collect()
                    .await;

                let mut fetched: Vec<(u64, JsonValue)> = Vec::with_capacity(results.len());
                for result in results {
                    fetched.push(result?);
                }
                fetched.sort_by_key(|(h, _)| *h);
                Ok::<_, ConnectorError>(fetched)
            };

            let logs_fut = async {
                if parse.logs {
                    let block_range = super::BlockRange::new(current, Some(batch_end));
                    self.inner
                        .get_logs(&block_range, None)
                        .await
                        .map_err(|e| {
                            ConnectorError::BlockchainRpc(format!(
                                "Failed to fetch logs for range {}-{}: {}",
                                current, batch_end, e
                            ))
                        })
                } else {
                    Ok(vec![])
                }
            };

            let (fetched_result, logs_result) = tokio::join!(blocks_fut, logs_fut);
            let mut fetched = fetched_result?;
            let logs = logs_result?;

            // Parse blocks and transactions from the block JSON.
            let num_blocks = fetched.len();
            let estimated_txs = num_blocks * 200;
            let mut block_rows = BlockRows::with_capacity(num_blocks);
            let mut tx_rows = TxRows::with_capacity(estimated_txs);

            for (block_height, block) in &mut fetched {
                let height = *block_height;
                // Extract block_hash once -- shared by both block and tx parsers.
                let block_hash = json_str_take(block, "hash");

                if parse.blocks {
                    parse_block(block, height, &block_hash, &mut block_rows);
                }

                if parse.transactions {
                    if let Some(txs) = block.get_mut("transactions").and_then(|v| v.as_array_mut())
                    {
                        for tx in txs {
                            parse_transaction(tx, height, &block_hash, &mut tx_rows);
                        }
                    }
                }
            }

            // Parse logs from the concurrent fetch.
            if parse.logs && !logs.is_empty() {
                let mut log_rows = LogRows::with_capacity(logs.len());
                for mut log in logs {
                    parse_log(&mut log, &mut log_rows);
                }
                if log_rows.len > 0 {
                    all_logs.push(log_rows.into_record_batch()?);
                }
            }

            if block_rows.len > 0 {
                all_blocks.push(block_rows.into_record_batch()?);
            }
            if tx_rows.len > 0 {
                all_txs.push(tx_rows.into_record_batch()?);
            }

            current = batch_end + 1;
        }

        Ok((all_blocks, all_txs, all_logs))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector trait implementation
// ═══════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Connector for EthereumConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Ethereum
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(eth_schema::ALL_TABLES
            .iter()
            .map(|&name| TableInfo {
                name: name.to_string(),
                schema: eth_schema::schema_for_table(name).unwrap(),
                supports_incremental: true,
                incremental_key: Some("block_number".to_string()),
                estimated_rows: None,
                primary_key_columns: eth_schema::primary_key_for_table(name),
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        eth_schema::schema_for_table(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown Ethereum table: {}", table))
        })
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        if eth_schema::schema_for_table(table).is_none() {
            return Err(ConnectorError::TableNotFound(format!(
                "Unknown Ethereum table: {}",
                table
            )));
        }

        let tip = self.inner.get_block_number().await?;

        let from_height = if incremental_key.is_some() {
            match last_value {
                Some(v) => v.parse::<u64>().unwrap_or(0) + 1,
                None => 0,
            }
        } else {
            0
        };

        if from_height > tip {
            return Ok(vec![]);
        }

        let parse = ParseTables::single(table);
        let (blocks, txs, logs) = self
            .fetch_block_range_filtered(from_height, tip, parse)
            .await?;

        match table {
            eth_schema::TABLE_BLOCKS => Ok(blocks),
            eth_schema::TABLE_TRANSACTIONS => Ok(txs),
            eth_schema::TABLE_LOGS => Ok(logs),
            _ => unreachable!("table was validated above"),
        }
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>,
    > {
        Box::pin(async move {
            let batches = self
                .fetch_table(
                    table,
                    options.incremental_key.as_deref(),
                    options.last_value.as_deref(),
                )
                .await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        self.inner.get_block_number().await.map(|_| ())
    }

    fn supports_cdc(&self) -> bool {
        false
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Hex parsing helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Parse a hex string (`"0x1a2b"`) into a `u64`.  Returns 0 on failure.
fn hex_to_u64(val: Option<&JsonValue>) -> u64 {
    val.and_then(|v| v.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}

/// Parse a hex string into an `i64`.  Returns 0 on failure.
fn hex_to_i64(val: Option<&JsonValue>) -> i64 {
    hex_to_u64(val) as i64
}

/// Parse a hex string into an `Option<i64>`.  Returns `None` if the field is
/// absent or null.
fn hex_to_opt_i64(val: Option<&JsonValue>) -> Option<i64> {
    val.and_then(|v| v.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .map(|v| v as i64)
}

/// Parse a hex string into an `i32`.  Returns 0 on failure.
fn hex_to_i32(val: Option<&JsonValue>) -> i32 {
    hex_to_u64(val) as i32
}

/// Extract a string field by taking ownership from the JSON value, avoiding
/// a copy.  The slot in `val` is replaced with `Null`.
fn json_str_take(val: &mut JsonValue, key: &str) -> String {
    match val.get_mut(key).map(|v| v.take()) {
        Some(JsonValue::String(s)) => s,
        _ => String::new(),
    }
}

/// Like `json_str_take` but returns `default` instead of an empty string
/// when the field is missing, null, or not a string.
fn json_str_take_or(val: &mut JsonValue, key: &str, default: &str) -> String {
    match val.get_mut(key).map(|v| v.take()) {
        Some(JsonValue::String(s)) => s,
        _ => default.to_string(),
    }
}

/// Extract an optional string field by taking ownership.
fn json_opt_str_take(val: &mut JsonValue, key: &str) -> Option<String> {
    match val.get_mut(key).map(|v| v.take()) {
        Some(JsonValue::String(s)) => Some(s),
        _ => None,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JSON-to-Arrow row builders
// ═══════════════════════════════════════════════════════════════════════════


// ── Block rows ───────────────────────────────────────────────────────────

struct BlockRows {
    block_number: Vec<i64>,
    block_hash: Vec<String>,
    parent_hash: Vec<String>,
    timestamp: Vec<i64>,
    miner: Vec<String>,
    gas_used: Vec<i64>,
    gas_limit: Vec<i64>,
    transaction_count: Vec<i32>,
    base_fee_per_gas: Vec<Option<i64>>,
    len: usize,
}

impl BlockRows {
    fn with_capacity(n: usize) -> Self {
        Self {
            block_number: Vec::with_capacity(n),
            block_hash: Vec::with_capacity(n),
            parent_hash: Vec::with_capacity(n),
            timestamp: Vec::with_capacity(n),
            miner: Vec::with_capacity(n),
            gas_used: Vec::with_capacity(n),
            gas_limit: Vec::with_capacity(n),
            transaction_count: Vec::with_capacity(n),
            base_fee_per_gas: Vec::with_capacity(n),
            len: 0,
        }
    }

    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::clone(&BLOCKS_ARROW_SCHEMA);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_number))),
            Arc::new(string_array(std::mem::take(&mut self.block_hash))),
            Arc::new(string_array(std::mem::take(&mut self.parent_hash))),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(std::mem::take(&mut self.timestamp))
                    .with_timezone("UTC"),
            ),
            Arc::new(string_array(std::mem::take(&mut self.miner))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.gas_used))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.gas_limit))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.transaction_count,
            ))),
            Arc::new(Int64Array::from(std::mem::take(
                &mut self.base_fee_per_gas,
            ))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build blocks batch: {}", e)))
    }
}

fn parse_block(block: &mut JsonValue, height: u64, block_hash: &str, rows: &mut BlockRows) {
    rows.block_number.push(height as i64);
    rows.block_hash.push(block_hash.to_string());
    rows.parent_hash.push(json_str_take(block, "parentHash"));
    let ts_secs = hex_to_u64(block.get("timestamp"));
    rows.timestamp.push((ts_secs as i64) * 1_000_000);
    rows.miner.push(json_str_take(block, "miner"));
    rows.gas_used.push(hex_to_i64(block.get("gasUsed")));
    rows.gas_limit.push(hex_to_i64(block.get("gasLimit")));
    let tx_count = block
        .get("transactions")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    rows.transaction_count.push(tx_count as i32);
    rows.base_fee_per_gas
        .push(hex_to_opt_i64(block.get("baseFeePerGas")));
    rows.len += 1;
}

// ── Transaction rows ─────────────────────────────────────────────────────

struct TxRows {
    tx_hash: Vec<String>,
    block_number: Vec<i64>,
    block_hash: Vec<String>,
    from_address: Vec<String>,
    to_address: Vec<Option<String>>,
    value: Vec<String>,
    gas: Vec<i64>,
    gas_price: Vec<Option<i64>>,
    input: Vec<String>,
    nonce: Vec<i64>,
    transaction_index: Vec<i32>,
    len: usize,
}

impl TxRows {
    fn with_capacity(n: usize) -> Self {
        Self {
            tx_hash: Vec::with_capacity(n),
            block_number: Vec::with_capacity(n),
            block_hash: Vec::with_capacity(n),
            from_address: Vec::with_capacity(n),
            to_address: Vec::with_capacity(n),
            value: Vec::with_capacity(n),
            gas: Vec::with_capacity(n),
            gas_price: Vec::with_capacity(n),
            input: Vec::with_capacity(n),
            nonce: Vec::with_capacity(n),
            transaction_index: Vec::with_capacity(n),
            len: 0,
        }
    }

    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::clone(&TXS_ARROW_SCHEMA);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(string_array(std::mem::take(&mut self.tx_hash))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_number))),
            Arc::new(string_array(std::mem::take(&mut self.block_hash))),
            Arc::new(string_array(std::mem::take(&mut self.from_address))),
            Arc::new(opt_string_array(std::mem::take(&mut self.to_address))),
            Arc::new(string_array(std::mem::take(&mut self.value))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.gas))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.gas_price))),
            Arc::new(string_array(std::mem::take(&mut self.input))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.nonce))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.transaction_index,
            ))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build txs batch: {}", e)))
    }
}

/// Parse a single transaction JSON into the row builder.
/// `block_hash` is extracted once per block by the caller to avoid redundant
/// string allocations for every transaction in the same block.
fn parse_transaction(tx: &mut JsonValue, height: u64, block_hash: &str, rows: &mut TxRows) {
    rows.tx_hash.push(json_str_take(tx, "hash"));
    rows.block_number.push(height as i64);
    rows.block_hash.push(block_hash.to_string());
    rows.from_address.push(json_str_take(tx, "from"));
    rows.to_address.push(json_opt_str_take(tx, "to"));
    rows.value.push(json_str_take_or(tx, "value", "0x0"));
    rows.gas.push(hex_to_i64(tx.get("gas")));
    rows.gas_price.push(hex_to_opt_i64(tx.get("gasPrice")));
    rows.input.push(json_str_take(tx, "input"));
    rows.nonce.push(hex_to_i64(tx.get("nonce")));
    rows.transaction_index
        .push(hex_to_i32(tx.get("transactionIndex")));
    rows.len += 1;
}

// ── Log rows ─────────────────────────────────────────────────────────────

struct LogRows {
    log_index: Vec<i32>,
    transaction_hash: Vec<String>,
    transaction_index: Vec<i32>,
    block_number: Vec<i64>,
    block_hash: Vec<String>,
    address: Vec<String>,
    topic0: Vec<Option<String>>,
    topic1: Vec<Option<String>>,
    topic2: Vec<Option<String>>,
    topic3: Vec<Option<String>>,
    data: Vec<String>,
    len: usize,
}

impl LogRows {
    fn with_capacity(n: usize) -> Self {
        Self {
            log_index: Vec::with_capacity(n),
            transaction_hash: Vec::with_capacity(n),
            transaction_index: Vec::with_capacity(n),
            block_number: Vec::with_capacity(n),
            block_hash: Vec::with_capacity(n),
            address: Vec::with_capacity(n),
            topic0: Vec::with_capacity(n),
            topic1: Vec::with_capacity(n),
            topic2: Vec::with_capacity(n),
            topic3: Vec::with_capacity(n),
            data: Vec::with_capacity(n),
            len: 0,
        }
    }

    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::clone(&LOGS_ARROW_SCHEMA);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(std::mem::take(&mut self.log_index))),
            Arc::new(string_array(std::mem::take(&mut self.transaction_hash))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.transaction_index,
            ))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_number))),
            Arc::new(string_array(std::mem::take(&mut self.block_hash))),
            Arc::new(string_array(std::mem::take(&mut self.address))),
            Arc::new(opt_string_array(std::mem::take(&mut self.topic0))),
            Arc::new(opt_string_array(std::mem::take(&mut self.topic1))),
            Arc::new(opt_string_array(std::mem::take(&mut self.topic2))),
            Arc::new(opt_string_array(std::mem::take(&mut self.topic3))),
            Arc::new(string_array(std::mem::take(&mut self.data))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build logs batch: {}", e)))
    }
}

fn parse_log(log: &mut JsonValue, rows: &mut LogRows) {
    rows.log_index.push(hex_to_i32(log.get("logIndex")));
    rows.transaction_hash
        .push(json_str_take(log, "transactionHash"));
    rows.transaction_index
        .push(hex_to_i32(log.get("transactionIndex")));
    rows.block_number
        .push(hex_to_i64(log.get("blockNumber")));
    rows.block_hash.push(json_str_take(log, "blockHash"));
    rows.address.push(json_str_take(log, "address"));

    // Take ownership of topic strings from the JSON array.
    if let Some(topics) = log.get_mut("topics").and_then(|v| v.as_array_mut()) {
        let take_topic = |topics: &mut Vec<JsonValue>, idx: usize| -> Option<String> {
            topics.get_mut(idx).and_then(|v| match v.take() {
                JsonValue::String(s) => Some(s),
                _ => None,
            })
        };
        rows.topic0.push(take_topic(topics, 0));
        rows.topic1.push(take_topic(topics, 1));
        rows.topic2.push(take_topic(topics, 2));
        rows.topic3.push(take_topic(topics, 3));
    } else {
        rows.topic0.push(None);
        rows.topic1.push(None);
        rows.topic2.push(None);
        rows.topic3.push(None);
    }

    rows.data.push(json_str_take(log, "data"));
    rows.len += 1;
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ethereum_config_new() {
        let cfg = EthereumConfig::new("https://eth.llamarpc.com");
        assert_eq!(cfg.rpc_url, "https://eth.llamarpc.com");
        assert_eq!(cfg.timeout_secs, 30);
        assert_eq!(cfg.batch_size, 50);
    }

    #[test]
    fn test_hex_to_u64() {
        assert_eq!(hex_to_u64(Some(&serde_json::json!("0x1"))), 1);
        assert_eq!(hex_to_u64(Some(&serde_json::json!("0xff"))), 255);
        assert_eq!(hex_to_u64(Some(&serde_json::json!("0x12a05f2"))), 19_531_250);
        assert_eq!(hex_to_u64(None), 0);
        assert_eq!(hex_to_u64(Some(&serde_json::json!(null))), 0);
    }

    #[test]
    fn test_hex_to_i64() {
        assert_eq!(hex_to_i64(Some(&serde_json::json!("0x1dcd6500"))), 500_000_000);
    }

    #[test]
    fn test_hex_to_opt_i64() {
        assert_eq!(hex_to_opt_i64(Some(&serde_json::json!("0xa"))), Some(10));
        assert_eq!(hex_to_opt_i64(None), None);
        assert_eq!(hex_to_opt_i64(Some(&serde_json::json!(null))), None);
    }

    #[test]
    fn test_parse_block_basic() {
        let mut block = serde_json::json!({
            "hash": "0xabc123",
            "parentHash": "0xdef456",
            "timestamp": "0x65d8c78c",
            "miner": "0x1234567890abcdef1234567890abcdef12345678",
            "gasUsed": "0xe4e1c0",
            "gasLimit": "0x1c9c380",
            "baseFeePerGas": "0x3b9aca00",
            "transactions": [
                {"hash": "0xtx1"},
                {"hash": "0xtx2"}
            ]
        });

        let mut rows = BlockRows::with_capacity(1);
        let block_hash = json_str_take(&mut block, "hash");
        parse_block(&mut block, 19_000_000, &block_hash, &mut rows);

        assert_eq!(rows.len, 1);
        assert_eq!(rows.block_number[0], 19_000_000);
        assert_eq!(rows.block_hash[0], "0xabc123");
        assert_eq!(rows.parent_hash[0], "0xdef456");
        assert_eq!(rows.transaction_count[0], 2);
        assert_eq!(rows.gas_used[0], 15_000_000);
        assert_eq!(rows.base_fee_per_gas[0], Some(1_000_000_000));

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_parse_transaction_basic() {
        let mut tx = serde_json::json!({
            "hash": "0xtxhash",
            "from": "0xsender",
            "to": "0xrecipient",
            "value": "0xde0b6b3a7640000",
            "gas": "0x5208",
            "gasPrice": "0x3b9aca00",
            "input": "0x",
            "nonce": "0x5",
            "transactionIndex": "0x0"
        });

        let mut rows = TxRows::with_capacity(1);
        parse_transaction(&mut tx, 100, "0xblockhash", &mut rows);

        assert_eq!(rows.len, 1);
        assert_eq!(rows.tx_hash[0], "0xtxhash");
        assert_eq!(rows.from_address[0], "0xsender");
        assert_eq!(rows.to_address[0], Some("0xrecipient".to_string()));
        assert_eq!(rows.value[0], "0xde0b6b3a7640000");
        assert_eq!(rows.gas[0], 21_000);
        assert_eq!(rows.nonce[0], 5);

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_parse_transaction_contract_creation() {
        let mut tx = serde_json::json!({
            "hash": "0xtxhash",
            "from": "0xsender",
            "to": null,
            "value": "0x0",
            "gas": "0x5208",
            "gasPrice": "0x3b9aca00",
            "input": "0x6060604052",
            "nonce": "0x0",
            "transactionIndex": "0x1"
        });

        let mut rows = TxRows::with_capacity(1);
        parse_transaction(&mut tx, 100, "0xblockhash", &mut rows);

        assert_eq!(rows.to_address[0], None);
    }

    #[test]
    fn test_parse_log_basic() {
        let mut log = serde_json::json!({
            "logIndex": "0x0",
            "transactionHash": "0xtxhash",
            "transactionIndex": "0x3",
            "blockNumber": "0x100",
            "blockHash": "0xblockhash",
            "address": "0xcontract",
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000sender",
                "0x000000000000000000000000receiver"
            ],
            "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"
        });

        let mut rows = LogRows::with_capacity(1);
        parse_log(&mut log, &mut rows);

        assert_eq!(rows.len, 1);
        assert_eq!(rows.log_index[0], 0);
        assert_eq!(rows.address[0], "0xcontract");
        assert!(rows.topic0[0].is_some());
        assert!(rows.topic1[0].is_some());
        assert!(rows.topic2[0].is_some());
        assert!(rows.topic3[0].is_none());

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_connector_list_tables() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let connector = EthereumConnector::new(EthereumConfig::new("https://eth.llamarpc.com"));
        let tables = rt.block_on(connector.list_tables()).unwrap();
        assert_eq!(tables.len(), 3);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"logs"));
    }

    // ── json_str_take tests ──────────────────────────────────────────

    #[test]
    fn test_json_str_take_moves_string() {
        let mut val = serde_json::json!({"key": "hello"});
        assert_eq!(json_str_take(&mut val, "key"), "hello");
        assert!(val.get("key").unwrap().is_null());
    }

    #[test]
    fn test_json_str_take_missing_key() {
        let mut val = serde_json::json!({});
        assert_eq!(json_str_take(&mut val, "key"), "");
    }

    #[test]
    fn test_json_str_take_null_value() {
        let mut val = serde_json::json!({"key": null});
        assert_eq!(json_str_take(&mut val, "key"), "");
    }

    #[test]
    fn test_json_str_take_non_string() {
        let mut val = serde_json::json!({"key": 42});
        assert_eq!(json_str_take(&mut val, "key"), "");
    }

    // ── json_str_take_or tests ───────────────────────────────────────

    #[test]
    fn test_json_str_take_or_present() {
        let mut val = serde_json::json!({"value": "0xabc"});
        assert_eq!(json_str_take_or(&mut val, "value", "0x0"), "0xabc");
        assert!(val.get("value").unwrap().is_null());
    }

    #[test]
    fn test_json_str_take_or_missing() {
        let mut val = serde_json::json!({});
        assert_eq!(json_str_take_or(&mut val, "value", "0x0"), "0x0");
    }

    #[test]
    fn test_json_str_take_or_null() {
        let mut val = serde_json::json!({"value": null});
        assert_eq!(json_str_take_or(&mut val, "value", "0x0"), "0x0");
    }

    // ── json_opt_str_take tests ──────────────────────────────────────

    #[test]
    fn test_json_opt_str_take_some() {
        let mut val = serde_json::json!({"key": "hello"});
        assert_eq!(json_opt_str_take(&mut val, "key"), Some("hello".to_string()));
        assert!(val.get("key").unwrap().is_null());
    }

    #[test]
    fn test_json_opt_str_take_null() {
        let mut val = serde_json::json!({"key": null});
        assert_eq!(json_opt_str_take(&mut val, "key"), None);
    }

    #[test]
    fn test_json_opt_str_take_missing() {
        let mut val = serde_json::json!({});
        assert_eq!(json_opt_str_take(&mut val, "key"), None);
    }

    #[test]
    fn test_json_opt_str_take_non_string() {
        let mut val = serde_json::json!({"key": 42});
        assert_eq!(json_opt_str_take(&mut val, "key"), None);
    }
}
