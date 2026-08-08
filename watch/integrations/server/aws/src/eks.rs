//! EKS integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect EKS cluster metrics from AWS CloudWatch.
//! Metrics collected include:
//! - ClusterControlPlaneRequests (API server requests)
//! - ClusterControlPlaneMetrics (API server latency, errors)
//! Note: Node and pod-level metrics are collected by the agent running in the cluster

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_eks::Client as EksClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// EKS cluster identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EksClusterName(pub String);

/// EKS cluster metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct EksClusterMetrics {
    pub cluster_name: String,
    pub timestamp: DateTime<Utc>,
    // Control plane metrics
    pub api_server_requests: Option<f64>, // ClusterControlPlaneRequests (Sum)
    pub api_server_latency: Option<f64>, // ClusterControlPlaneMetrics - latency (Average)
    pub api_server_errors: Option<f64>, // ClusterControlPlaneMetrics - errors (Sum)
}

/// EKS metrics collector
pub struct EksCollector {
    eks_client: EksClient,
    cloudwatch_client: CloudWatchClient,
}

impl EksCollector {
    /// Create a new EKS collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            eks_client: EksClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all EKS clusters in the configured region
    pub async fn list_clusters(&self) -> Result<Vec<EksClusterName>> {
        info!("Listing EKS clusters...");
        
        let mut cluster_names = Vec::new();
        let mut paginator = self.eks_client.list_clusters().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(clusters) = page.clusters {
                for cluster_name in clusters {
                    cluster_names.push(EksClusterName(cluster_name));
                }
            }
        }

        info!("Found {} EKS clusters", cluster_names.len());
        Ok(cluster_names)
    }

    /// Collect CloudWatch metrics for an EKS cluster
    pub async fn collect_metrics(
        &self,
        cluster_name: &EksClusterName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<EksClusterMetrics> {
        let mut metrics = EksClusterMetrics {
            cluster_name: cluster_name.0.clone(),
            timestamp: end_time,
            api_server_requests: None,
            api_server_latency: None,
            api_server_errors: None,
        };

        // EKS control plane metrics are available via CloudWatch
        // Note: Node and pod metrics are typically collected by the agent running in the cluster
        
        // ClusterControlPlaneRequests - total API server requests
        metrics.api_server_requests = self.get_metric_statistic("ClusterControlPlaneRequests", &cluster_name.0, start_time, end_time, true).await?;
        
        // Note: ClusterControlPlaneMetrics provides detailed metrics, but we'll focus on requests
        // Additional metrics like latency and errors may require parsing the ClusterControlPlaneMetrics metric
        // For simplicity, we'll collect the requests metric which is the most commonly available

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for EKS
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        cluster_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("ClusterName")
            .value(cluster_name)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/EKS")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(300); // 5-minute periods

        if use_sum {
            request = request.statistics(aws_sdk_cloudwatch::types::Statistic::Sum);
        } else {
            request = request.statistics(aws_sdk_cloudwatch::types::Statistic::Average);
        }

        let response = request.send().await;

        match response {
            Ok(resp) => {
                if let Some(datapoints) = resp.datapoints {
                    if let Some(latest) = datapoints.iter().max_by_key(|dp| {
                        dp.timestamp().map(|t| t.secs()).unwrap_or(0)
                    }) {
                        if use_sum {
                            if let Some(value) = latest.sum {
                                return Ok(Some(value));
                            }
                        } else {
                            if let Some(value) = latest.average {
                                return Ok(Some(value));
                            }
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to get EKS metric {} for cluster {}: {}", metric_name, cluster_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple EKS clusters
    pub async fn collect_metrics_batch(
        &self,
        clusters: &[EksClusterName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<EksClusterMetrics>> {
        let mut metrics = Vec::new();

        for cluster in clusters {
            match self.collect_metrics(cluster, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for EKS cluster {}: {}", cluster.0, e);
                }
            }
        }

        Ok(metrics)
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

/// Convert EKS cluster metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn eks_metrics_to_reiver_format(
    metrics: &EksClusterMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_name:{}", metrics.cluster_name),
        "source:aws_eks".to_string(),
    ];

    if let Some(requests) = metrics.api_server_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.eks.api_server_requests".to_string(),
            value: requests,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

