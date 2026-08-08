use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{instrument, info};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use serde_json::Value;

use crate::config::DatabaseConfig;
use crate::metrics::Collector;

pub struct ElasticsearchCollector {
    config: Arc<DatabaseConfig>,
    client: reqwest::Client,
    base_url: String,
}

impl ElasticsearchCollector {
    #[instrument(skip(config), fields(database = %config.name, host = %config.host))]
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        // Build base URL
        // Elasticsearch uses HTTP REST API, not SQL
        let protocol = if config.port == 443 || config.port == 9200 && config.host.contains("https") {
            "https"
        } else {
            "http"
        };
        
        let base_url = format!("{}://{}:{}", protocol, config.host, config.port);
        
        info!("Initializing Elasticsearch collector...");
        
        // Create HTTP client
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("Failed to create HTTP client for Elasticsearch")?;
        
        // Test connection - Elasticsearch health endpoint
        let health_url = format!("{}/_cluster/health", base_url);
        let response = client.get(&health_url).send().await
            .context(format!("Failed to connect to Elasticsearch: {}", config.name))?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Elasticsearch health check failed: {}", response.status()));
        }

        info!("Connected to Elasticsearch");

        Ok(Self {
            config,
            client,
            base_url,
        })
    }

    /// Query Elasticsearch cluster stats via REST API (sync version for callbacks)
    fn query_cluster_stats_sync(base_url: &str, config: &DatabaseConfig) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime for Elasticsearch query")?;
        
        rt.block_on(async {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .context("Failed to create HTTP client")?;
            
            let mut metrics = Vec::new();
            
            let base_tags = vec![
                KeyValue::new("host", config.host.clone()),
                KeyValue::new("source", "remote"),
                KeyValue::new("database", config.name.clone()),
                KeyValue::new("db_type", "elasticsearch"),
            ];
            
            // Get cluster health (simpler endpoint with status)
            let health_url = format!("{}/_cluster/health", base_url);
            if let Ok(response) = client.get(&health_url).send().await {
                if response.status().is_success() {
                    if let Ok(health) = response.json::<serde_json::Value>().await {
                        // Extract status
                        if let Some(status_val) = health.get("status").and_then(|v| v.as_str()) {
                            let status_num = match status_val {
                                "green" => 2.0,
                                "yellow" => 1.0,
                                "red" => 0.0,
                                _ => 0.0,
                            };
                            metrics.push(("elasticsearch.cluster.status".to_string(), status_num, base_tags.clone()));
                        }
                        
                        // Extract number of nodes
                        if let Some(nodes_val) = health.get("number_of_nodes").and_then(|v| v.as_u64()) {
                            metrics.push(("elasticsearch.cluster.nodes".to_string(), nodes_val as f64, base_tags.clone()));
                        }
                        
                        // Extract number of indices
                        if let Some(indices_val) = health.get("number_of_indices").and_then(|v| v.as_u64()) {
                            metrics.push(("elasticsearch.cluster.indices.count".to_string(), indices_val as f64, base_tags.clone()));
                        }
                    }
                }
            }
            
            // Get cluster stats for more detailed metrics
            let stats_url = format!("{}/_cluster/stats", base_url);
            if let Ok(response) = client.get(&stats_url).send().await {
                if response.status().is_success() {
                    if let Ok(stats) = response.json::<serde_json::Value>().await {
                        // Extract metrics from cluster stats
                        Self::extract_metrics_from_json(&stats, "elasticsearch.cluster".to_string(), &base_tags, &mut metrics);
                    }
                }
            }
            
            // Get node stats
            let node_stats_url = format!("{}/_nodes/stats", base_url);
            if let Ok(response) = client.get(&node_stats_url).send().await {
                if response.status().is_success() {
                    if let Ok(node_stats) = response.json::<serde_json::Value>().await {
                        // Extract node metrics
                        if let Some(nodes) = node_stats.get("nodes").and_then(|n| n.as_object()) {
                            for (node_id, node_data) in nodes {
                                let mut node_tags = base_tags.clone();
                                node_tags.push(KeyValue::new("node_id", node_id.clone()));
                                Self::extract_metrics_from_json(node_data, "elasticsearch.node".to_string(), &node_tags, &mut metrics);
                            }
                        }
                    }
                }
            }
            
            Ok::<_, anyhow::Error>(metrics)
        })
    }

    /// Query Elasticsearch cluster stats via REST API
    fn query_cluster_stats(&self) -> Result<Vec<(String, f64, Vec<KeyValue>)>> {
        Self::query_cluster_stats_sync(&self.base_url, &self.config)
    }

    /// Recursively extract metrics from JSON
    fn extract_metrics_from_json(
        json: &Value,
        prefix: String,
        base_tags: &[KeyValue],
        metrics: &mut Vec<(String, f64, Vec<KeyValue>)>,
    ) {
        match json {
            Value::Number(n) => {
                if let Some(v) = n.as_f64() {
                    metrics.push((prefix, v, base_tags.to_vec()));
                }
            }
            Value::Object(map) => {
                for (key, value) in map {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    Self::extract_metrics_from_json(value, new_prefix, base_tags, metrics);
                }
            }
            _ => {} // Skip other types
        }
    }
}

impl Collector for ElasticsearchCollector {
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()> {
        if !self.config.query_metrics.enabled {
            return Ok(());
        }
        
        // Clone what we need for the callbacks
        let config = self.config.clone();
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        
        // Common Elasticsearch metrics using the sync query method
        let _cluster_status = meter
            .f64_observable_gauge("elasticsearch.cluster.status")
            .with_description("Cluster status (0=red, 1=yellow, 2=green)")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_cluster_stats_sync(&base_url, &config) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "elasticsearch"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "elasticsearch.cluster.status" {
                                observer.observe(value, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();
        
        let _indices_count = meter
            .u64_observable_gauge("elasticsearch.cluster.indices.count")
            .with_description("Number of indices")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_cluster_stats_sync(&base_url, &config) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "elasticsearch"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "elasticsearch.cluster.indices.count" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();
        
        let _nodes_count = meter
            .u64_observable_gauge("elasticsearch.cluster.nodes")
            .with_description("Number of nodes in cluster")
            .with_callback({
                let config = config.clone();
                let base_url = base_url.clone();
                move |observer| {
                    if let Ok(metrics) = Self::query_cluster_stats_sync(&base_url, &config) {
                        let tags = vec![
                            KeyValue::new("host", config.host.clone()),
                            KeyValue::new("source", "remote"),
                            KeyValue::new("database", config.name.clone()),
                            KeyValue::new("db_type", "elasticsearch"),
                        ];
                        for (name, value, _) in metrics {
                            if name == "elasticsearch.cluster.nodes" {
                                observer.observe(value as u64, &tags);
                                break;
                            }
                        }
                    }
                }
            })
            .build();
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.config.name
    }
    
    fn enabled(&self) -> bool {
        self.config.enabled && self.config.query_metrics.enabled
    }
}

