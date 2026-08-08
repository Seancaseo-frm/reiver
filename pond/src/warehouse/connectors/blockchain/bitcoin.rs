//! Bitcoin connector — implements the `Connector` trait for bitcoind full nodes.
//!
//! Connects via JSON-RPC, exposes four tables (blocks, transactions, inputs,
//! outputs), and uses `block_height` as the incremental key for height-based
//! syncing.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::arrow_utils::{opt_string_array, string_array, to_arrow_schema};
use super::rpc::BitcoinRpcClient;
use super::schema;
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};

/// Controls which tables are parsed when processing raw block JSON.
///
/// Used to avoid wasting CPU/memory on tables the caller does not need.
#[derive(Debug, Clone, Copy)]
struct ParseTables {
    blocks: bool,
    transactions: bool,
    inputs: bool,
    outputs: bool,
}

impl ParseTables {
    /// Parse all four tables (used by `fetch_block_range`).
    fn all() -> Self {
        Self { blocks: true, transactions: true, inputs: true, outputs: true }
    }

    /// Parse only the given table (used by `fetch_table`).
    fn single(name: &str) -> Self {
        Self {
            blocks: name == schema::TABLE_BLOCKS,
            transactions: name == schema::TABLE_TRANSACTIONS,
            inputs: name == schema::TABLE_INPUTS,
            outputs: name == schema::TABLE_OUTPUTS,
        }
    }

    fn any_tx_level(&self) -> bool {
        self.transactions || self.inputs || self.outputs
    }
}

/// Bitcoin network variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitcoinNetwork {
    Mainnet,
    Testnet,
    Signet,
    Regtest,
}

impl Default for BitcoinNetwork {
    fn default() -> Self {
        Self::Mainnet
    }
}

/// Configuration for the Bitcoin connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinConfig {
    pub rpc_url: String,
    #[serde(default)]
    pub rpc_user: Option<String>,
    #[serde(default)]
    pub rpc_password: Option<String>,
    #[serde(default)]
    pub network: BitcoinNetwork,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_timeout() -> u64 { 60 }
fn default_batch_size() -> u64 { 10 }
fn default_max_retries() -> u32 { 3 }

impl BitcoinConfig {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            rpc_user: None,
            rpc_password: None,
            network: BitcoinNetwork::default(),
            timeout_secs: default_timeout(),
            batch_size: default_batch_size(),
            max_retries: default_max_retries(),
        }
    }
}

/// Bitcoin connector that fetches block data from a full node.
pub struct BitcoinConnector {
    config: BitcoinConfig,
    rpc: BitcoinRpcClient,
}

impl std::fmt::Debug for BitcoinConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitcoinConnector")
            .field("rpc_url", &self.config.rpc_url)
            .field("network", &self.config.network)
            .finish()
    }
}

impl BitcoinConnector {
    pub fn new(config: BitcoinConfig) -> Self {
        let rpc = BitcoinRpcClient::new(
            &config.rpc_url,
            config.rpc_user.as_deref(),
            config.rpc_password.as_deref(),
            Duration::from_secs(config.timeout_secs),
            config.max_retries,
        );
        Self { config, rpc }
    }

    /// Fetch blocks in the given height range and convert them into
    /// `RecordBatch` vectors keyed by table name.
    ///
    /// Returns `(blocks_batches, txs_batches, inputs_batches, outputs_batches)`.
    pub async fn fetch_block_range(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> ConnectorResult<(Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>)>
    {
        self.fetch_block_range_filtered(from_height, to_height, ParseTables::all())
            .await
    }

    /// Internal: fetch blocks and only parse the tables indicated by `parse`.
    ///
    /// Blocks within each batch are fetched concurrently (up to 8 in flight)
    /// to overlap RPC latency, then sorted by height for deterministic output.
    async fn fetch_block_range_filtered(
        &self,
        from_height: u64,
        to_height: u64,
        parse: ParseTables,
    ) -> ConnectorResult<(Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>)>
    {
        const MAX_CONCURRENT: usize = 8;

        let mut all_blocks = Vec::new();
        let mut all_txs = Vec::new();
        let mut all_inputs = Vec::new();
        let mut all_outputs = Vec::new();

        let batch_size = self.config.batch_size;
        let concurrency = (batch_size as usize).min(MAX_CONCURRENT);
        let mut current = from_height;

        while current <= to_height {
            let batch_end = std::cmp::min(current + batch_size - 1, to_height);

            // Fetch blocks concurrently within this batch.
            let fetch_results: Vec<ConnectorResult<(u64, String, JsonValue)>> =
                stream::iter((current..=batch_end).map(|height| async move {
                    let hash = self.rpc.get_block_hash(height).await?;
                    let block = self.rpc.get_block(&hash).await?;
                    Ok((height, hash, block))
                }))
                .buffer_unordered(concurrency)
                .collect()
                .await;

            // Collect results and sort by height for deterministic output.
            let mut fetched: Vec<(u64, String, JsonValue)> =
                Vec::with_capacity(fetch_results.len());
            for result in fetch_results {
                fetched.push(result?);
            }
            fetched.sort_by_key(|(h, _, _)| *h);

            // Parse in height order.
            let mut block_rows = BlockRows::default();
            let mut tx_rows = TxRows::default();
            let mut input_rows = InputRows::default();
            let mut output_rows = OutputRows::default();

            for (height, hash, block) in &fetched {
                if parse.blocks {
                    parse_block(block, *height, &mut block_rows);
                }

                if parse.any_tx_level() {
                    if let Some(txs) = block.get("tx").and_then(|v| v.as_array()) {
                        for tx in txs {
                            if parse.transactions {
                                parse_transaction(tx, *height, hash, &mut tx_rows);
                            }
                            if parse.inputs {
                                parse_inputs(tx, *height, &mut input_rows);
                            }
                            if parse.outputs {
                                parse_outputs(tx, *height, &mut output_rows);
                            }
                        }
                    }
                }
            }

            if block_rows.len > 0 {
                all_blocks.push(block_rows.into_record_batch()?);
            }
            if tx_rows.len > 0 {
                all_txs.push(tx_rows.into_record_batch()?);
            }
            if input_rows.len > 0 {
                all_inputs.push(input_rows.into_record_batch()?);
            }
            if output_rows.len > 0 {
                all_outputs.push(output_rows.into_record_batch()?);
            }

            current = batch_end + 1;
        }

        Ok((all_blocks, all_txs, all_inputs, all_outputs))
    }

    /// Expose the underlying RPC client for use by the global sync daemon.
    pub fn rpc(&self) -> &BitcoinRpcClient {
        &self.rpc
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connector trait implementation
// ═══════════════════════════════════════════════════════════════════════════

#[async_trait]
impl Connector for BitcoinConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Bitcoin
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        Ok(schema::ALL_TABLES
            .iter()
            .map(|&name| TableInfo {
                name: name.to_string(),
                schema: schema::schema_for_table(name).unwrap(),
                supports_incremental: true,
                incremental_key: Some("block_height".to_string()),
                estimated_rows: None,
                primary_key_columns: schema::primary_key_for_table(name),
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        schema::schema_for_table(table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("Unknown Bitcoin table: {}", table))
        })
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        // Validate table name early to avoid fetching data for an unknown table.
        if schema::schema_for_table(table).is_none() {
            return Err(ConnectorError::TableNotFound(format!(
                "Unknown Bitcoin table: {}",
                table
            )));
        }

        let tip = self.rpc.get_block_count().await?;

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

        // Only parse the single table the caller needs.
        let parse = ParseTables::single(table);
        let (blocks, txs, inputs, outputs) =
            self.fetch_block_range_filtered(from_height, tip, parse).await?;

        match table {
            schema::TABLE_BLOCKS => Ok(blocks),
            schema::TABLE_TRANSACTIONS => Ok(txs),
            schema::TABLE_INPUTS => Ok(inputs),
            schema::TABLE_OUTPUTS => Ok(outputs),
            _ => unreachable!("table was validated above"),
        }
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
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
        self.rpc.get_blockchain_info().await.map(|_| ())
    }

    fn supports_cdc(&self) -> bool {
        false
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JSON-to-Arrow row builders
// ═══════════════════════════════════════════════════════════════════════════


// ── Block rows ───────────────────────────────────────────────────────────

#[derive(Default)]
struct BlockRows {
    block_height: Vec<i64>,
    block_hash: Vec<String>,
    previous_block_hash: Vec<Option<String>>,
    timestamp: Vec<i64>,
    size: Vec<i64>,
    weight: Vec<i64>,
    version: Vec<i32>,
    nonce: Vec<i64>,
    difficulty: Vec<f64>,
    merkle_root: Vec<String>,
    num_transactions: Vec<i32>,
    stripped_size: Vec<i64>,
    len: usize,
}

fn parse_block(block: &JsonValue, height: u64, rows: &mut BlockRows) {
    rows.block_height.push(height as i64);
    rows.block_hash
        .push(json_str(block, "hash").unwrap_or_default());
    rows.previous_block_hash
        .push(json_str(block, "previousblockhash"));
    rows.timestamp
        .push(block.get("time").and_then(|v| v.as_i64()).unwrap_or(0) * 1_000_000);
    rows.size
        .push(block.get("size").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.weight
        .push(block.get("weight").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.version
        .push(block.get("version").and_then(|v| v.as_i64()).unwrap_or(0) as i32);
    rows.nonce
        .push(block.get("nonce").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.difficulty.push(
        block
            .get("difficulty")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
    );
    rows.merkle_root
        .push(json_str(block, "merkleroot").unwrap_or_default());
    rows.num_transactions.push(
        block
            .get("nTx")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32,
    );
    rows.stripped_size.push(
        block
            .get("strippedsize")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
    );
    rows.len += 1;
}

impl BlockRows {
    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::new(to_arrow_schema(&schema::blocks_schema()));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_height))),
            Arc::new(string_array(std::mem::take(&mut self.block_hash))),
            Arc::new(opt_string_array(std::mem::take(&mut self.previous_block_hash))),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(std::mem::take(&mut self.timestamp)).with_timezone("UTC")),
            Arc::new(Int64Array::from(std::mem::take(&mut self.size))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.weight))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.version))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.nonce))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.difficulty))),
            Arc::new(string_array(std::mem::take(&mut self.merkle_root))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.num_transactions))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.stripped_size))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build blocks batch: {}", e)))
    }
}

// ── Transaction rows ─────────────────────────────────────────────────────

#[derive(Default)]
struct TxRows {
    txid: Vec<String>,
    block_height: Vec<i64>,
    block_hash: Vec<String>,
    size: Vec<i64>,
    vsize: Vec<i64>,
    weight: Vec<i64>,
    version: Vec<i32>,
    locktime: Vec<i64>,
    fee: Vec<Option<i64>>,
    is_coinbase: Vec<bool>,
    input_count: Vec<i32>,
    output_count: Vec<i32>,
    input_value: Vec<Option<i64>>,
    output_value: Vec<i64>,
    len: usize,
}

fn parse_transaction(tx: &JsonValue, height: u64, block_hash: &str, rows: &mut TxRows) {
    rows.txid
        .push(json_str(tx, "txid").unwrap_or_default());
    rows.block_height.push(height as i64);
    rows.block_hash.push(block_hash.to_string());
    rows.size
        .push(tx.get("size").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.vsize
        .push(tx.get("vsize").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.weight
        .push(tx.get("weight").and_then(|v| v.as_i64()).unwrap_or(0));
    rows.version
        .push(tx.get("version").and_then(|v| v.as_i64()).unwrap_or(0) as i32);
    rows.locktime
        .push(tx.get("locktime").and_then(|v| v.as_i64()).unwrap_or(0));

    let vin = tx.get("vin").and_then(|v| v.as_array());
    let vout = tx.get("vout").and_then(|v| v.as_array());

    let is_cb = vin
        .map(|v| v.first().map(|i| i.get("coinbase").is_some()).unwrap_or(false))
        .unwrap_or(false);
    rows.is_coinbase.push(is_cb);

    rows.input_count
        .push(vin.map(|v| v.len()).unwrap_or(0) as i32);
    rows.output_count
        .push(vout.map(|v| v.len()).unwrap_or(0) as i32);

    let total_out: i64 = vout
        .map(|outputs| {
            outputs.iter().map(|o| satoshis_from_btc(o.get("value"))).sum()
        })
        .unwrap_or(0);
    rows.output_value.push(total_out);

    if is_cb {
        rows.fee.push(None);
        rows.input_value.push(None);
    } else {
        let total_in: i64 = vin
            .map(|inputs| {
                inputs
                    .iter()
                    .map(|i| {
                        i.get("prevout")
                            .and_then(|po| po.get("value"))
                            .and_then(|v| v.as_f64())
                            .map(|btc| (btc * 100_000_000.0).round() as i64)
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0);
        rows.input_value.push(Some(total_in));
        rows.fee.push(Some(total_in - total_out));
    }

    rows.len += 1;
}

impl TxRows {
    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::new(to_arrow_schema(&schema::transactions_schema()));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(string_array(std::mem::take(&mut self.txid))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_height))),
            Arc::new(string_array(std::mem::take(&mut self.block_hash))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.size))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.vsize))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.weight))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.version))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.locktime))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.fee))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_coinbase))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.input_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.output_count))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.input_value))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.output_value))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build txs batch: {}", e)))
    }
}

// ── Input rows ───────────────────────────────────────────────────────────

#[derive(Default)]
struct InputRows {
    txid: Vec<String>,
    input_index: Vec<i32>,
    block_height: Vec<i64>,
    prev_txid: Vec<Option<String>>,
    prev_output_index: Vec<Option<i32>>,
    script_sig: Vec<Option<String>>,
    sequence: Vec<i64>,
    witness: Vec<Option<String>>,
    value: Vec<Option<i64>>,
    is_coinbase: Vec<bool>,
    len: usize,
}

fn parse_inputs(tx: &JsonValue, height: u64, rows: &mut InputRows) {
    let txid = json_str(tx, "txid").unwrap_or_default();
    let vin = match tx.get("vin").and_then(|v| v.as_array()) {
        Some(v) => v,
        None => return,
    };

    for (idx, input) in vin.iter().enumerate() {
        let is_cb = input.get("coinbase").is_some();
        rows.txid.push(txid.clone());
        rows.input_index.push(idx as i32);
        rows.block_height.push(height as i64);

        if is_cb {
            rows.prev_txid.push(None);
            rows.prev_output_index.push(None);
            rows.script_sig.push(json_str(input, "coinbase"));
            rows.value.push(None);
        } else {
            rows.prev_txid.push(json_str(input, "txid"));
            rows.prev_output_index
                .push(input.get("vout").and_then(|v| v.as_i64()).map(|v| v as i32));
            rows.script_sig.push(
                input
                    .get("scriptSig")
                    .and_then(|s| s.get("hex"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            );
            rows.value.push(
                input
                    .get("prevout")
                    .and_then(|po| po.get("value"))
                    .and_then(|v| v.as_f64())
                    .map(|btc| (btc * 100_000_000.0).round() as i64),
            );
        }

        rows.sequence
            .push(input.get("sequence").and_then(|v| v.as_i64()).unwrap_or(0));

        let witness_data = input
            .get("txinwitness")
            .and_then(|v| v.as_array())
            .map(|arr| serde_json::to_string(arr).unwrap_or_default());
        rows.witness.push(witness_data);

        rows.is_coinbase.push(is_cb);
        rows.len += 1;
    }
}

impl InputRows {
    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::new(to_arrow_schema(&schema::inputs_schema()));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(string_array(std::mem::take(&mut self.txid))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.input_index))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_height))),
            Arc::new(opt_string_array(std::mem::take(&mut self.prev_txid))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.prev_output_index))),
            Arc::new(opt_string_array(std::mem::take(&mut self.script_sig))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.sequence))),
            Arc::new(opt_string_array(std::mem::take(&mut self.witness))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.value))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_coinbase))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Internal(format!("Failed to build inputs batch: {}", e)))
    }
}

// ── Output rows ──────────────────────────────────────────────────────────

#[derive(Default)]
struct OutputRows {
    txid: Vec<String>,
    output_index: Vec<i32>,
    block_height: Vec<i64>,
    value_satoshis: Vec<i64>,
    script_pubkey: Vec<String>,
    script_type: Vec<Option<String>>,
    address: Vec<Option<String>>,
    required_signatures: Vec<Option<i32>>,
    len: usize,
}

fn parse_outputs(tx: &JsonValue, height: u64, rows: &mut OutputRows) {
    let txid = json_str(tx, "txid").unwrap_or_default();
    let vout = match tx.get("vout").and_then(|v| v.as_array()) {
        Some(v) => v,
        None => return,
    };

    for (idx, output) in vout.iter().enumerate() {
        rows.txid.push(txid.clone());
        rows.output_index.push(idx as i32);
        rows.block_height.push(height as i64);
        rows.value_satoshis
            .push(satoshis_from_btc(output.get("value")));

        let spk = output.get("scriptPubKey");
        rows.script_pubkey.push(
            spk.and_then(|s| s.get("hex"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        );
        rows.script_type.push(
            spk.and_then(|s| s.get("type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        );
        rows.address.push(
            spk.and_then(|s| s.get("address"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        );
        rows.required_signatures.push(
            spk.and_then(|s| s.get("reqSigs"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
        );
        rows.len += 1;
    }
}

impl OutputRows {
    fn into_record_batch(mut self) -> ConnectorResult<RecordBatch> {
        let schema = Arc::new(to_arrow_schema(&schema::outputs_schema()));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(string_array(std::mem::take(&mut self.txid))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.output_index))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.block_height))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.value_satoshis))),
            Arc::new(string_array(std::mem::take(&mut self.script_pubkey))),
            Arc::new(opt_string_array(std::mem::take(&mut self.script_type))),
            Arc::new(opt_string_array(std::mem::take(&mut self.address))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.required_signatures))),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to build outputs batch: {}", e))
            })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn json_str(val: &JsonValue, key: &str) -> Option<String> {
    val.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn satoshis_from_btc(val: Option<&JsonValue>) -> i64 {
    val.and_then(|v| v.as_f64())
        .map(|btc| (btc * 100_000_000.0).round() as i64)
        .unwrap_or(0)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitcoin_config_new() {
        let cfg = BitcoinConfig::new("http://127.0.0.1:8332");
        assert_eq!(cfg.rpc_url, "http://127.0.0.1:8332");
        assert_eq!(cfg.network, BitcoinNetwork::Mainnet);
        assert_eq!(cfg.timeout_secs, 60);
    }

    #[test]
    fn test_satoshis_from_btc() {
        let val = serde_json::json!(1.5);
        assert_eq!(satoshis_from_btc(Some(&val)), 150_000_000);
        assert_eq!(satoshis_from_btc(None), 0);
    }

    #[test]
    fn test_satoshis_from_btc_rounding() {
        // 0.1 BTC cannot be exactly represented in f64; without .round() this
        // would truncate to 9_999_999 instead of the correct 10_000_000.
        let val = serde_json::json!(0.1);
        assert_eq!(satoshis_from_btc(Some(&val)), 10_000_000);

        let val = serde_json::json!(0.00000001);
        assert_eq!(satoshis_from_btc(Some(&val)), 1);

        let val = serde_json::json!(21000000.0);
        assert_eq!(satoshis_from_btc(Some(&val)), 2_100_000_000_000_000);
    }

    #[test]
    fn test_parse_block_basic() {
        let block = serde_json::json!({
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            "previousblockhash": null,
            "time": 1231006505,
            "size": 285,
            "weight": 1140,
            "version": 1,
            "nonce": 2083236893,
            "difficulty": 1.0,
            "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            "nTx": 1,
            "strippedsize": 285,
            "tx": []
        });

        let mut rows = BlockRows::default();
        parse_block(&block, 0, &mut rows);

        assert_eq!(rows.len, 1);
        assert_eq!(rows.block_height[0], 0);
        assert_eq!(rows.nonce[0], 2083236893);
        assert_eq!(rows.num_transactions[0], 1);

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
    }

    #[test]
    fn test_parse_transaction_coinbase() {
        let tx = serde_json::json!({
            "txid": "abc123",
            "size": 200,
            "vsize": 180,
            "weight": 720,
            "version": 1,
            "locktime": 0,
            "vin": [{"coinbase": "04ffff001d0104", "sequence": 4294967295u64}],
            "vout": [{"value": 50.0, "scriptPubKey": {"hex": "aabb", "type": "pubkey"}}]
        });

        let mut rows = TxRows::default();
        parse_transaction(&tx, 0, "blockhash", &mut rows);

        assert_eq!(rows.len, 1);
        assert!(rows.is_coinbase[0]);
        assert_eq!(rows.fee[0], None);
        assert_eq!(rows.output_value[0], 5_000_000_000);

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parse_outputs_basic() {
        let tx = serde_json::json!({
            "txid": "tx1",
            "vout": [
                {
                    "value": 0.5,
                    "scriptPubKey": {
                        "hex": "76a914...",
                        "type": "pubkeyhash",
                        "address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
                    }
                },
                {
                    "value": 0.3,
                    "scriptPubKey": {
                        "hex": "a914...",
                        "type": "scripthash",
                        "address": "3EktnHQD7RiAE6uzMj2ZifT9YgRnMLkPba"
                    }
                }
            ]
        });

        let mut rows = OutputRows::default();
        parse_outputs(&tx, 100, &mut rows);

        assert_eq!(rows.len, 2);
        assert_eq!(rows.value_satoshis[0], 50_000_000);
        assert_eq!(rows.value_satoshis[1], 30_000_000);
        assert_eq!(rows.address[0].as_deref(), Some("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"));

        let batch = rows.into_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_connector_list_tables() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let connector = BitcoinConnector::new(BitcoinConfig::new("http://localhost:8332"));
        let tables = rt.block_on(connector.list_tables()).unwrap();
        assert_eq!(tables.len(), 4);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"inputs"));
        assert!(names.contains(&"outputs"));
    }
}
