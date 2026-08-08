//! Azure Kubernetes Service (AKS) integration for collecting Azure Monitor metrics
//!
//! This module provides functionality to collect AKS cluster-level metrics from Azure Monitor.
//! Metrics collected include:
//! - Node Count
//! - System Node Count
//! - User Node Count
//! - Cluster Status
//! 
//! Note: Node and pod-level metrics should be collected by an agent running within the cluster.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AzureConfig;

/// Azure AKS Cluster identifier (resource ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureAksClusterId(pub String);

/// Azure AKS cluster metrics collected from Azure Monitor and Resource Manager
#[derive(Debug, Clone, Serialize)]
pub struct AzureAksMetrics {
    pub cluster_id: String,
    pub cluster_name: String,
    pub resource_group: String,
    pub timestamp: DateTime<Utc>,
    pub node_count: Option<f64>,
    pub system_node_count: Option<f64>,
    pub user_node_count: Option<f64>,
    pub total_cpu_cores: Option<f64>,
    pub total_memory_gb: Option<f64>,
}

/// Azure Monitor Metrics API response structures (reused from compute module)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AzureMonitorMetricsResponse {
    value: Vec<AzureMonitorMetric>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorMetric {
    id: String,
    #[serde(rename = "type")]
    metric_type: String,
    name: AzureMonitorMetricName,
    displayDescription: Option<String>,
    unit: String,
    timeseries: Vec<AzureMonitorTimeSeries>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorMetricName {
    value: String,
    localizedValue: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorTimeSeries {
    #[serde(default)]
    metadatavalues: Vec<serde_json::Value>,
    data: Vec<AzureMonitorDataPoint>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct AzureMonitorDataPoint {
    timeStamp: String,
    average: Option<f64>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    total: Option<f64>,
    count: Option<f64>,
}

/// AKS cluster resource details from Resource Manager API
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AksClusterResource {
    id: Option<String>,
    name: Option<String>,
    #[serde(rename = "type")]
    resource_type: Option<String>,
    properties: Option<AksClusterProperties>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AksClusterProperties {
    #[serde(rename = "agentPoolProfiles")]
    agent_pool_profiles: Option<Vec<AgentPoolProfile>>,
    #[serde(rename = "powerState")]
    power_state: Option<PowerState>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AgentPoolProfile {
    name: Option<String>,
    count: Option<i32>,
    #[serde(rename = "vmSize")]
    vm_size: Option<String>,
    #[serde(rename = "osType")]
    os_type: Option<String>,
    #[serde(rename = "mode")]
    mode: Option<String>, // System or User
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct PowerState {
    code: Option<String>,
}

/// Azure AKS metrics collector
pub struct AzureAksCollector {
    config: AzureConfig,
    http_client: Client,
}

impl AzureAksCollector {
    /// Create a new Azure AKS collector with the given Azure configuration
    pub fn new(config: AzureConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all AKS clusters in the subscription
    pub async fn list_clusters(&self) -> Result<Vec<AzureAksClusterId>> {
        info!("Listing Azure AKS clusters...");
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        let url = format!(
            "https://management.azure.com/subscriptions/{}/resources?$filter=resourceType eq 'Microsoft.ContainerService/managedClusters'&api-version=2023-03-01",
            self.config.subscription_id
        );
        
        let mut all_clusters = Vec::new();
        let mut next_link: Option<String> = Some(url);
        
        while let Some(link) = next_link {
            let response = self.http_client
                .get(&link)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Azure AKS clusters: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Azure API error ({}): {}", status, body));
            }
            
            let resource_list: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse Azure API response: {}", e))?;
            
            // Extract AKS cluster resource IDs
            if let Some(resources) = resource_list.get("value").and_then(|v| v.as_array()) {
                for resource in resources {
                    if let Some(id) = resource.get("id").and_then(|v| v.as_str()) {
                        all_clusters.push(AzureAksClusterId(id.to_string()));
                    }
                }
            }
            
            // Check for next page
            next_link = resource_list.get("nextLink")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        
        info!("Found {} Azure AKS clusters", all_clusters.len());
        Ok(all_clusters)
    }

    /// Extract resource group name from Azure resource ID
    fn extract_resource_group(resource_id: &str) -> Option<String> {
        let parts: Vec<&str> = resource_id.split('/').collect();
        if parts.len() >= 4 && parts[1] == "subscriptions" && parts[3] == "resourceGroups" {
            Some(parts[4].to_string())
        } else {
            None
        }
    }

    /// Extract AKS cluster name from Azure resource ID
    fn extract_cluster_name(resource_id: &str) -> String {
        resource_id.split('/').last().unwrap_or(resource_id).to_string()
    }

    /// Get VM size CPU and memory (approximate, based on common VM sizes)
    fn vm_size_to_resources(vm_size: &str) -> (Option<f64>, Option<f64>) {
        // Common VM sizes mapping (simplified - in production, use Azure VM size API or comprehensive mapping)
        let vm_size_lower = vm_size.to_lowercase();
        if vm_size_lower.contains("standard_d2s_v3") {
            (Some(2.0), Some(8.0)) // 2 vCPUs, 8 GB
        } else if vm_size_lower.contains("standard_d4s_v3") {
            (Some(4.0), Some(16.0)) // 4 vCPUs, 16 GB
        } else if vm_size_lower.contains("standard_d8s_v3") {
            (Some(8.0), Some(32.0)) // 8 vCPUs, 32 GB
        } else if vm_size_lower.contains("standard_d16s_v3") {
            (Some(16.0), Some(64.0)) // 16 vCPUs, 64 GB
        } else {
            (None, None) // Unknown VM size
        }
    }

    /// Collect cluster information from Resource Manager API and metrics from Azure Monitor
    pub async fn collect_metrics(
        &self,
        cluster_id: &AzureAksClusterId,
        _start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<AzureAksMetrics> {
        info!("Collecting Azure AKS metrics for: {}", cluster_id.0);
        
        let cluster_name = Self::extract_cluster_name(&cluster_id.0);
        let resource_group = Self::extract_resource_group(&cluster_id.0)
            .unwrap_or_else(|| "unknown".to_string());
        
        let access_token = self.config
            .get_access_token(AzureConfig::monitor_scope())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Azure access token: {}", e))?;
        
        // First, get cluster details from Resource Manager API to get node pool information
        let cluster_url = format!(
            "https://management.azure.com{}?api-version=2023-03-01",
            cluster_id.0
        );
        
        let cluster_response = self.http_client
            .get(&cluster_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get AKS cluster details: {}", e))?;
        
        let mut node_count: Option<f64> = None;
        let mut system_node_count: Option<f64> = None;
        let mut user_node_count: Option<f64> = None;
        let mut total_cpu_cores: Option<f64> = None;
        let mut total_memory_gb: Option<f64> = None;
        
        if cluster_response.status().is_success() {
            let cluster_resource: AksClusterResource = cluster_response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse AKS cluster details: {}", e))?;
            
            // Calculate node counts and resources from agent pool profiles
            if let Some(properties) = cluster_resource.properties {
                if let Some(agent_pools) = properties.agent_pool_profiles {
                    let mut total_nodes = 0;
                    let mut system_nodes = 0;
                    let mut user_nodes = 0;
                    let mut total_cpu = 0.0;
                    let mut total_memory = 0.0;
                    
                    for pool in agent_pools {
                        let pool_count = pool.count.unwrap_or(0) as f64;
                        total_nodes += pool.count.unwrap_or(0);
                        
                        // Determine if system or user pool
                        if pool.mode.as_deref() == Some("System") {
                            system_nodes += pool.count.unwrap_or(0);
                        } else {
                            user_nodes += pool.count.unwrap_or(0);
                        }
                        
                        // Calculate total resources based on VM size
                        if let Some(vm_size) = pool.vm_size {
                            let (cpu, memory) = Self::vm_size_to_resources(&vm_size);
                            if let Some(cpu_per_node) = cpu {
                                total_cpu += pool_count * cpu_per_node;
                            }
                            if let Some(memory_per_node) = memory {
                                total_memory += pool_count * memory_per_node;
                            }
                        }
                    }
                    
                    if total_nodes > 0 {
                        node_count = Some(total_nodes as f64);
                    }
                    if system_nodes > 0 {
                        system_node_count = Some(system_nodes as f64);
                    }
                    if user_nodes > 0 {
                        user_node_count = Some(user_nodes as f64);
                    }
                    if total_cpu > 0.0 {
                        total_cpu_cores = Some(total_cpu);
                    }
                    if total_memory > 0.0 {
                        total_memory_gb = Some(total_memory);
                    }
                }
            }
        } else {
            warn!("Failed to get AKS cluster details for {}, status: {}", cluster_id.0, cluster_response.status());
        }
        
        // Build metrics API URL for any additional metrics from Azure Monitor
        // Note: Most AKS metrics are collected via Azure Monitor for Containers (in-cluster agent)
        // This collector focuses on cluster-level infrastructure metrics
        
        Ok(AzureAksMetrics {
            cluster_id: cluster_id.0.clone(),
            cluster_name: cluster_name.clone(),
            resource_group,
            timestamp: end_time,
            node_count,
            system_node_count,
            user_node_count,
            total_cpu_cores,
            total_memory_gb,
        })
    }

    /// Collect metrics for multiple AKS clusters in parallel
    pub async fn collect_metrics_batch(
        &self,
        clusters: &[AzureAksClusterId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<AzureAksMetrics>> {
        let mut tasks = Vec::new();
        for cluster_id in clusters {
            let collector = self.clone();
            let cluster_id_clone = cluster_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&cluster_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Azure AKS cluster: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for AzureAksCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            http_client: Client::new(),
        }
    }
}

/// Reiver metric format (compatible with metrics API)
#[derive(Debug, Clone, Serialize)]
pub struct ReiverMetric {
    pub name: String,
    pub value: f64,
    #[serde(rename = "type")]
    pub r#type: String,
    pub timestamp: DateTime<Utc>,
    pub tags: Vec<String>,
}

/// Convert Azure AKS metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn azure_aks_metrics_to_reiver_format(
    metrics: &AzureAksMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_id:{}", metrics.cluster_id),
        format!("cluster_name:{}", metrics.cluster_name),
        format!("resource_group:{}", metrics.resource_group),
        "source:azure_aks".to_string(),
    ];

    if let Some(value) = metrics.node_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.aks.node_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.system_node_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.aks.system_node_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.user_node_count {
        reiver_metrics.push(ReiverMetric {
            name: "azure.aks.user_node_count".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_cpu_cores {
        reiver_metrics.push(ReiverMetric {
            name: "azure.aks.total_cpu_cores".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_memory_gb {
        reiver_metrics.push(ReiverMetric {
            name: "azure.aks.total_memory_gb".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
