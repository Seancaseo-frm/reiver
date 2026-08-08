//! Blockchain Connector Base
//!
//! Provides base implementations for blockchain data sources.
//!
//! # Supported Chains
//!
//! - Ethereum (and EVM-compatible chains)
//! - Solana
//! - Bitcoin
//! - Polygon
//!
//! # Features
//!
//! - Block range queries
//! - Transaction filtering
//! - Event/log parsing
//! - Contract ABI decoding
//! - Multi-chain support

pub mod arrow_utils;
pub mod bitcoin;
pub mod eth_schema;
pub mod ethereum;
pub mod rpc;
pub mod schema;

pub use bitcoin::{BitcoinConnector, BitcoinConfig, BitcoinNetwork};
pub use ethereum::{EthereumConnector, EthereumConfig};

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{ConnectorError, ConnectorResult};
use crate::warehouse::types::{TableSchema, ColumnSchema, ColumnType};

/// Blockchain types supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockchainType {
    /// Ethereum mainnet
    Ethereum,
    /// Solana
    Solana,
    /// Bitcoin
    Bitcoin,
    /// Polygon (Matic)
    Polygon,
    /// Arbitrum
    Arbitrum,
    /// Optimism
    Optimism,
    /// Base
    Base,
    /// Avalanche C-Chain
    Avalanche,
    /// BNB Smart Chain
    BnbChain,
}

impl std::fmt::Display for BlockchainType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockchainType::Ethereum => write!(f, "ethereum"),
            BlockchainType::Solana => write!(f, "solana"),
            BlockchainType::Bitcoin => write!(f, "bitcoin"),
            BlockchainType::Polygon => write!(f, "polygon"),
            BlockchainType::Arbitrum => write!(f, "arbitrum"),
            BlockchainType::Optimism => write!(f, "optimism"),
            BlockchainType::Base => write!(f, "base"),
            BlockchainType::Avalanche => write!(f, "avalanche"),
            BlockchainType::BnbChain => write!(f, "bnb_chain"),
        }
    }
}

impl BlockchainType {
    /// Get the default RPC endpoint for this chain.
    pub fn default_rpc_url(&self) -> Option<&'static str> {
        match self {
            BlockchainType::Ethereum => Some("https://eth.llamarpc.com"),
            BlockchainType::Polygon => Some("https://polygon-rpc.com"),
            BlockchainType::Arbitrum => Some("https://arb1.arbitrum.io/rpc"),
            BlockchainType::Optimism => Some("https://mainnet.optimism.io"),
            BlockchainType::Base => Some("https://mainnet.base.org"),
            BlockchainType::Avalanche => Some("https://api.avax.network/ext/bc/C/rpc"),
            BlockchainType::BnbChain => Some("https://bsc-dataseed.binance.org"),
            BlockchainType::Solana => Some("https://api.mainnet-beta.solana.com"),
            BlockchainType::Bitcoin => None, // Bitcoin uses different RPC
        }
    }

    /// Check if this is an EVM-compatible chain.
    pub fn is_evm(&self) -> bool {
        matches!(
            self,
            BlockchainType::Ethereum
                | BlockchainType::Polygon
                | BlockchainType::Arbitrum
                | BlockchainType::Optimism
                | BlockchainType::Base
                | BlockchainType::Avalanche
                | BlockchainType::BnbChain
        )
    }
}

/// Blockchain RPC client configuration.
#[derive(Debug, Clone)]
pub struct BlockchainConfig {
    /// Chain type
    pub chain: BlockchainType,
    /// RPC endpoint URL
    pub rpc_url: String,
    /// Optional API key for the RPC provider
    pub api_key: Option<String>,
    /// Request timeout
    pub timeout: Duration,
    /// Maximum retries for RPC calls
    pub max_retries: u32,
    /// Batch size for block fetching
    pub batch_size: u64,
}

impl BlockchainConfig {
    /// Create a new blockchain configuration.
    pub fn new(chain: BlockchainType, rpc_url: impl Into<String>) -> Self {
        Self {
            chain,
            rpc_url: rpc_url.into(),
            api_key: None,
            timeout: Duration::from_secs(30),
            max_retries: 3,
            batch_size: 100,
        }
    }

    /// Create a configuration with the default RPC endpoint.
    pub fn with_default_rpc(chain: BlockchainType) -> Option<Self> {
        chain.default_rpc_url().map(|url| Self::new(chain, url))
    }

    /// Set API key.
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Set timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set batch size.
    pub fn with_batch_size(mut self, size: u64) -> Self {
        self.batch_size = size;
        self
    }
}

/// Block range for queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRange {
    /// Start block (inclusive)
    pub from_block: u64,
    /// End block (inclusive), None means latest
    pub to_block: Option<u64>,
}

impl BlockRange {
    /// Create a new block range.
    pub fn new(from_block: u64, to_block: Option<u64>) -> Self {
        Self { from_block, to_block }
    }

    /// Create a range for a single block.
    pub fn single_block(block: u64) -> Self {
        Self {
            from_block: block,
            to_block: Some(block),
        }
    }
}

/// Contract filter for event queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractFilter {
    /// Contract address (hex string with 0x prefix)
    pub address: String,
    /// Event signatures to filter (hex string with 0x prefix)
    pub event_signatures: Vec<String>,
    /// ABI for decoding events (optional JSON)
    pub abi: Option<String>,
}

impl ContractFilter {
    /// Create a new contract filter.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            event_signatures: Vec::new(),
            abi: None,
        }
    }

    /// Add event signatures to filter.
    pub fn with_events(mut self, signatures: Vec<String>) -> Self {
        self.event_signatures = signatures;
        self
    }

    /// Add ABI for event decoding.
    pub fn with_abi(mut self, abi: impl Into<String>) -> Self {
        self.abi = Some(abi.into());
        self
    }
}

/// Base blockchain connector for RPC-based data fetching.
pub struct BlockchainConnector {
    config: BlockchainConfig,
    client: reqwest::Client,
}

impl std::fmt::Debug for BlockchainConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockchainConnector")
            .field("chain", &self.config.chain)
            .field("rpc_url", &self.config.rpc_url)
            .finish()
    }
}

impl BlockchainConnector {
    /// Create a new blockchain connector.
    pub fn new(config: BlockchainConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self { config, client }
    }

    /// Configured batch size for block fetching.
    pub fn batch_size(&self) -> u64 {
        self.config.batch_size
    }

    /// RPC endpoint URL.
    pub fn rpc_url(&self) -> &str {
        &self.config.rpc_url
    }

    /// Make a JSON-RPC call to the blockchain node.
    pub async fn rpc_call(
        &self,
        method: &str,
        params: Vec<JsonValue>,
    ) -> ConnectorResult<JsonValue> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut attempts: u32 = 0;
        let mut last_error = None;

        while attempts <= self.config.max_retries {
            // Exponential backoff with jitter on retries (skip on first attempt).
            if attempts > 0 {
                let base_ms = 200u64.saturating_mul(1u64 << attempts.min(6));
                let jitter_ms = base_ms / 4;
                let jitter = if jitter_ms > 0 {
                    (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .subsec_nanos() as u64)
                        % jitter_ms
                } else {
                    0
                };
                tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
            }

            let response = self
                .client
                .post(&self.config.rpc_url)
                .json(&request)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_server_error() {
                        // 5xx: transient server error, worth retrying.
                        last_error = Some(ConnectorError::BlockchainRpc(format!(
                            "HTTP error: {}",
                            resp.status()
                        )));
                        attempts += 1;
                        continue;
                    } else if !resp.status().is_success() {
                        // 4xx: deterministic client error, retry won't help.
                        return Err(ConnectorError::BlockchainRpc(format!(
                            "HTTP error: {}",
                            resp.status()
                        )));
                    }

                    let body: JsonValue = resp.json().await.map_err(|e| {
                        ConnectorError::BlockchainRpc(format!("Failed to parse response: {}", e))
                    })?;

                    if let Some(error) = body.get("error") {
                        return Err(ConnectorError::BlockchainRpc(format!(
                            "RPC error: {}",
                            error
                        )));
                    }

                    return body.get("result").cloned().ok_or_else(|| {
                        ConnectorError::BlockchainRpc("Missing result in response".to_string())
                    });
                }
                Err(e) => {
                    last_error = Some(ConnectorError::Network(format!("RPC request failed: {}", e)));
                    attempts += 1;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ConnectorError::BlockchainRpc("RPC call failed after retries".to_string())
        }))
    }

    /// Get the current block number.
    pub async fn get_block_number(&self) -> ConnectorResult<u64> {
        if self.config.chain == BlockchainType::Solana {
            let result = self.rpc_call("getSlot", vec![]).await?;
            return result.as_u64().ok_or_else(|| {
                ConnectorError::BlockchainRpc("Invalid slot number".to_string())
            });
        }

        let result = self.rpc_call("eth_blockNumber", vec![]).await?;
        let hex_str = result.as_str().ok_or_else(|| {
            ConnectorError::BlockchainRpc("Invalid block number format".to_string())
        })?;

        u64::from_str_radix(hex_str.trim_start_matches("0x"), 16).map_err(|e| {
            ConnectorError::BlockchainRpc(format!("Failed to parse block number: {}", e))
        })
    }

    /// Get a block by number (with full transaction bodies).
    pub async fn get_block(&self, block_number: u64) -> ConnectorResult<JsonValue> {
        if self.config.chain == BlockchainType::Solana {
            return self
                .rpc_call(
                    "getBlock",
                    vec![
                        serde_json::json!(block_number),
                        serde_json::json!({
                            "encoding": "json",
                            "transactionDetails": "full",
                            "rewards": false
                        }),
                    ],
                )
                .await;
        }

        let block_hex = format!("0x{:x}", block_number);
        self.rpc_call(
            "eth_getBlockByNumber",
            vec![serde_json::json!(block_hex), serde_json::json!(true)],
        )
        .await
    }

    /// Get a block header by number (without transaction bodies).
    ///
    /// Much lighter than `get_block` — returns only the block header fields
    /// (hash, parentHash, timestamp, etc.) without embedding the full
    /// transaction objects.  Use this when you only need metadata like the
    /// block hash.
    pub async fn get_block_header(&self, block_number: u64) -> ConnectorResult<JsonValue> {
        let block_hex = format!("0x{:x}", block_number);
        self.rpc_call(
            "eth_getBlockByNumber",
            vec![serde_json::json!(block_hex), serde_json::json!(false)],
        )
        .await
    }

    /// Get logs/events for a block range.
    pub async fn get_logs(
        &self,
        block_range: &BlockRange,
        contract_filter: Option<&ContractFilter>,
    ) -> ConnectorResult<Vec<JsonValue>> {
        if !self.config.chain.is_evm() {
            return Err(ConnectorError::UnsupportedFormat(
                "get_logs is only supported for EVM chains".to_string(),
            ));
        }

        let from_block = format!("0x{:x}", block_range.from_block);
        let to_block = block_range
            .to_block
            .map(|b| format!("0x{:x}", b))
            .unwrap_or_else(|| "latest".to_string());

        let mut filter = serde_json::json!({
            "fromBlock": from_block,
            "toBlock": to_block,
        });

        if let Some(cf) = contract_filter {
            filter["address"] = serde_json::json!(cf.address);
            if !cf.event_signatures.is_empty() {
                filter["topics"] = serde_json::json!([cf.event_signatures.clone()]);
            }
        }

        let result = self.rpc_call("eth_getLogs", vec![filter]).await?;

        result
            .as_array()
            .cloned()
            .ok_or_else(|| ConnectorError::BlockchainRpc("Invalid logs format".to_string()))
    }

    /// Get transaction by hash.
    pub async fn get_transaction(&self, tx_hash: &str) -> ConnectorResult<JsonValue> {
        if self.config.chain == BlockchainType::Solana {
            return self
                .rpc_call(
                    "getTransaction",
                    vec![
                        serde_json::json!(tx_hash),
                        serde_json::json!({ "encoding": "json" }),
                    ],
                )
                .await;
        }

        self.rpc_call("eth_getTransactionByHash", vec![serde_json::json!(tx_hash)])
            .await
    }

    /// Get account balance.
    pub async fn get_balance(&self, address: &str) -> ConnectorResult<String> {
        if self.config.chain == BlockchainType::Solana {
            let result = self
                .rpc_call("getBalance", vec![serde_json::json!(address)])
                .await?;
            return Ok(result["value"].to_string());
        }

        let result = self
            .rpc_call(
                "eth_getBalance",
                vec![serde_json::json!(address), serde_json::json!("latest")],
            )
            .await?;

        result.as_str().map(|s| s.to_string()).ok_or_else(|| {
            ConnectorError::BlockchainRpc("Invalid balance format".to_string())
        })
    }
}

/// Schema for EVM blocks.
pub fn evm_blocks_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("block_number", ColumnType::Int64, false)
                .with_description("Block number"),
            ColumnSchema::new("block_hash", ColumnType::String, false)
                .with_description("Block hash"),
            ColumnSchema::new("parent_hash", ColumnType::String, false)
                .with_description("Parent block hash"),
            ColumnSchema::new("timestamp", ColumnType::Timestamp, false)
                .with_description("Block timestamp")
                .with_timezone("UTC"),
            ColumnSchema::new("miner", ColumnType::String, false)
                .with_description("Block miner/validator address"),
            ColumnSchema::new("gas_used", ColumnType::Int64, false)
                .with_description("Total gas used in block"),
            ColumnSchema::new("gas_limit", ColumnType::Int64, false)
                .with_description("Block gas limit"),
            ColumnSchema::new("transaction_count", ColumnType::Int32, false)
                .with_description("Number of transactions in block"),
            ColumnSchema::new("base_fee_per_gas", ColumnType::Int64, true)
                .with_description("Base fee per gas (EIP-1559)"),
        ],
    }
}

/// Schema for EVM transactions.
pub fn evm_transactions_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("tx_hash", ColumnType::String, false)
                .with_description("Transaction hash"),
            ColumnSchema::new("block_number", ColumnType::Int64, false)
                .with_description("Block number"),
            ColumnSchema::new("block_hash", ColumnType::String, false)
                .with_description("Block hash"),
            ColumnSchema::new("from_address", ColumnType::String, false)
                .with_description("Sender address"),
            ColumnSchema::new("to_address", ColumnType::String, true)
                .with_description("Recipient address (null for contract creation)"),
            ColumnSchema::new("value", ColumnType::String, false)
                .with_description("Value transferred in wei"),
            ColumnSchema::new("gas", ColumnType::Int64, false)
                .with_description("Gas limit"),
            ColumnSchema::new("gas_price", ColumnType::Int64, true)
                .with_description("Gas price"),
            ColumnSchema::new("input", ColumnType::String, false)
                .with_description("Transaction input data"),
            ColumnSchema::new("nonce", ColumnType::Int64, false)
                .with_description("Transaction nonce"),
            ColumnSchema::new("transaction_index", ColumnType::Int32, false)
                .with_description("Index in block"),
        ],
    }
}

/// Schema for EVM logs/events.
pub fn evm_logs_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema::new("log_index", ColumnType::Int32, false)
                .with_description("Log index in block"),
            ColumnSchema::new("transaction_hash", ColumnType::String, false)
                .with_description("Transaction hash"),
            ColumnSchema::new("transaction_index", ColumnType::Int32, false)
                .with_description("Transaction index in block"),
            ColumnSchema::new("block_number", ColumnType::Int64, false)
                .with_description("Block number"),
            ColumnSchema::new("block_hash", ColumnType::String, false)
                .with_description("Block hash"),
            ColumnSchema::new("address", ColumnType::String, false)
                .with_description("Contract address that emitted the log"),
            ColumnSchema::new("topic0", ColumnType::String, true)
                .with_description("Event signature (first topic)"),
            ColumnSchema::new("topic1", ColumnType::String, true)
                .with_description("Second topic (indexed param)"),
            ColumnSchema::new("topic2", ColumnType::String, true)
                .with_description("Third topic (indexed param)"),
            ColumnSchema::new("topic3", ColumnType::String, true)
                .with_description("Fourth topic (indexed param)"),
            ColumnSchema::new("data", ColumnType::String, false)
                .with_description("Non-indexed event data"),
        ],
    }
}

/// Blockchain source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainSourceConfig {
    /// Blockchain type
    pub chain: BlockchainType,
    /// Contract addresses to monitor
    #[serde(default)]
    pub contracts: Vec<String>,
    /// Starting block number (None = latest)
    pub start_block: Option<u64>,
    /// Sync batch size
    #[serde(default = "default_blockchain_batch_size")]
    pub batch_size: u64,
    /// Tables to sync (blocks, transactions, logs)
    #[serde(default)]
    pub tables: Vec<String>,
}

fn default_blockchain_batch_size() -> u64 { 100 }

impl Default for BlockchainSourceConfig {
    fn default() -> Self {
        Self {
            chain: BlockchainType::Ethereum,
            contracts: Vec::new(),
            start_block: None,
            batch_size: default_blockchain_batch_size(),
            tables: vec!["blocks".to_string(), "transactions".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockchain_type_display() {
        assert_eq!(BlockchainType::Ethereum.to_string(), "ethereum");
        assert_eq!(BlockchainType::Polygon.to_string(), "polygon");
        assert_eq!(BlockchainType::Solana.to_string(), "solana");
    }

    #[test]
    fn test_blockchain_type_is_evm() {
        assert!(BlockchainType::Ethereum.is_evm());
        assert!(BlockchainType::Polygon.is_evm());
        assert!(BlockchainType::Arbitrum.is_evm());
        assert!(!BlockchainType::Solana.is_evm());
        assert!(!BlockchainType::Bitcoin.is_evm());
    }

    #[test]
    fn test_blockchain_config_creation() {
        let config = BlockchainConfig::new(BlockchainType::Ethereum, "https://rpc.example.com");
        assert_eq!(config.chain, BlockchainType::Ethereum);
        assert_eq!(config.rpc_url, "https://rpc.example.com");
    }

    #[test]
    fn test_blockchain_config_with_default_rpc() {
        let config = BlockchainConfig::with_default_rpc(BlockchainType::Ethereum);
        assert!(config.is_some());
        
        let config = BlockchainConfig::with_default_rpc(BlockchainType::Bitcoin);
        assert!(config.is_none());
    }

    #[test]
    fn test_block_range() {
        let range = BlockRange::new(100, Some(200));
        assert_eq!(range.from_block, 100);
        assert_eq!(range.to_block, Some(200));
    }

    #[test]
    fn test_block_range_single() {
        let range = BlockRange::single_block(12345);
        assert_eq!(range.from_block, 12345);
        assert_eq!(range.to_block, Some(12345));
    }

    #[test]
    fn test_contract_filter() {
        let filter = ContractFilter::new("0x1234...")
            .with_events(vec!["0xddf252...".to_string()]);
        assert_eq!(filter.address, "0x1234...");
        assert_eq!(filter.event_signatures.len(), 1);
    }

    #[test]
    fn test_evm_blocks_schema() {
        let schema = evm_blocks_schema();
        assert!(!schema.columns.is_empty());
        
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"block_number"));
        assert!(column_names.contains(&"block_hash"));
        assert!(column_names.contains(&"timestamp"));
    }

    #[test]
    fn test_evm_transactions_schema() {
        let schema = evm_transactions_schema();
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"tx_hash"));
        assert!(column_names.contains(&"from_address"));
        assert!(column_names.contains(&"to_address"));
    }

    #[test]
    fn test_evm_logs_schema() {
        let schema = evm_logs_schema();
        let column_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"address"));
        assert!(column_names.contains(&"topic0"));
        assert!(column_names.contains(&"data"));
    }

    #[test]
    fn test_blockchain_source_config_default() {
        let config = BlockchainSourceConfig::default();
        assert_eq!(config.chain, BlockchainType::Ethereum);
        assert_eq!(config.batch_size, 100);
    }
}
