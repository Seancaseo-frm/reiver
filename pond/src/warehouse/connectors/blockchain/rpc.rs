//! Lightweight Bitcoin JSON-RPC client.
//!
//! Wraps `reqwest` to call bitcoind's JSON-RPC API with optional
//! HTTP basic authentication and automatic retry on transient failures.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::warehouse::connectors::{ConnectorError, ConnectorResult};

/// Bitcoin JSON-RPC client.
pub struct BitcoinRpcClient {
    client: reqwest::Client,
    url: String,
    next_id: AtomicU64,
    max_retries: u32,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Vec<JsonValue>,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<JsonValue>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl BitcoinRpcClient {
    /// Create a new Bitcoin RPC client.
    pub fn new(
        url: impl Into<String>,
        user: Option<&str>,
        password: Option<&str>,
        timeout: Duration,
        max_retries: u32,
    ) -> Self {
        let mut builder = reqwest::Client::builder().timeout(timeout);

        if let (Some(u), Some(p)) = (user, password) {
            builder = builder.default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                let credentials = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    format!("{}:{}", u, p),
                );
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Basic {}", credentials).parse().unwrap(),
                );
                headers
            });
        }

        let client = builder.build().expect("Failed to create HTTP client");

        Self {
            client,
            url: url.into(),
            next_id: AtomicU64::new(1),
            max_retries,
        }
    }

    /// Send a JSON-RPC request with retry logic.
    async fn call(&self, method: &str, params: Vec<JsonValue>) -> ConnectorResult<JsonValue> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = RpcRequest {
            jsonrpc: "2.0",
            id,
            method,
            params,
        };

        let body = serde_json::to_vec(&request).map_err(|e| {
            ConnectorError::BlockchainRpc(format!("Failed to serialize request: {}", e))
        })?;

        let mut attempts: u32 = 0;
        let mut last_error = None;

        while attempts <= self.max_retries {
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

            let resp = self
                .client
                .post(&self.url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await;

            match resp {
                Ok(response) => {
                    if response.status().is_server_error() {
                        // 5xx: transient server error, worth retrying.
                        last_error = Some(ConnectorError::BlockchainRpc(format!(
                            "HTTP {}", response.status()
                        )));
                        attempts += 1;
                        continue;
                    } else if !response.status().is_success() {
                        // 4xx: deterministic client error, retry won't help.
                        return Err(ConnectorError::BlockchainRpc(format!(
                            "HTTP {}", response.status()
                        )));
                    }

                    let rpc_resp: RpcResponse = response.json().await.map_err(|e| {
                        ConnectorError::BlockchainRpc(format!("Failed to parse response: {}", e))
                    })?;

                    if let Some(err) = rpc_resp.error {
                        return Err(ConnectorError::BlockchainRpc(format!(
                            "RPC error {}: {}", err.code, err.message
                        )));
                    }

                    return rpc_resp.result.ok_or_else(|| {
                        ConnectorError::BlockchainRpc("Missing result in response".to_string())
                    });
                }
                Err(e) => {
                    last_error =
                        Some(ConnectorError::Network(format!("RPC request failed: {}", e)));
                    attempts += 1;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ConnectorError::BlockchainRpc("RPC call failed after retries".to_string())
        }))
    }

    // ── Bitcoin-specific RPC wrappers ────────────────────────────────

    /// Return the current block count (chain tip height).
    pub async fn get_block_count(&self) -> ConnectorResult<u64> {
        let result = self.call("getblockcount", vec![]).await?;
        result.as_u64().ok_or_else(|| {
            ConnectorError::BlockchainRpc("Invalid block count".to_string())
        })
    }

    /// Return the block hash at the given height.
    pub async fn get_block_hash(&self, height: u64) -> ConnectorResult<String> {
        let result = self
            .call("getblockhash", vec![serde_json::json!(height)])
            .await?;
        result
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| ConnectorError::BlockchainRpc("Invalid block hash".to_string()))
    }

    /// Return full block data with decoded transactions (verbosity=2).
    pub async fn get_block(&self, hash: &str) -> ConnectorResult<JsonValue> {
        self.call(
            "getblock",
            vec![serde_json::json!(hash), serde_json::json!(2)],
        )
        .await
    }

    /// Return high-level blockchain info.
    pub async fn get_blockchain_info(&self) -> ConnectorResult<JsonValue> {
        self.call("getblockchaininfo", vec![]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_client_creation() {
        let client = BitcoinRpcClient::new(
            "http://127.0.0.1:8332",
            Some("user"),
            Some("pass"),
            Duration::from_secs(30),
            3,
        );
        assert_eq!(client.url, "http://127.0.0.1:8332");
        assert_eq!(client.max_retries, 3);
    }

    #[test]
    fn test_rpc_client_no_auth() {
        let client = BitcoinRpcClient::new(
            "http://127.0.0.1:8332",
            None,
            None,
            Duration::from_secs(30),
            3,
        );
        assert_eq!(client.url, "http://127.0.0.1:8332");
    }
}
