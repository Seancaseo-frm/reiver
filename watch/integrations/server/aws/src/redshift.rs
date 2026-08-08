//! Redshift integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect Redshift cluster metrics from AWS CloudWatch.
//! Metrics collected include:
//! - CPUUtilization
//! - DatabaseConnections
//! - WLMQueueLength (Workload Management queue length)
//! - ReadLatency/WriteLatency
//! - ReadThroughput/WriteThroughput
//! - NetworkReceiveThroughput/NetworkTransmitThroughput
//! - HealthStatus
//! - MaintenanceMode
//! - PercentageOfDiskSpaceUsed

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_redshift::Client as RedshiftClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// Redshift cluster identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedshiftClusterId(pub String);

/// Redshift metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct RedshiftMetrics {
    pub cluster_id: String,
    pub cluster_identifier: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub database_connections: Option<f64>,
    pub wlm_queue_length: Option<f64>,
    pub read_latency: Option<f64>,
    pub write_latency: Option<f64>,
    pub read_throughput: Option<f64>,
    pub write_throughput: Option<f64>,
    pub network_receive_throughput: Option<f64>,
    pub network_transmit_throughput: Option<f64>,
    pub health_status: Option<String>,
    pub maintenance_mode: Option<bool>,
    pub percentage_of_disk_space_used: Option<f64>,
}

/// Redshift metrics collector
pub struct RedshiftCollector {
    redshift_client: RedshiftClient,
    cloudwatch_client: CloudWatchClient,
}

impl RedshiftCollector {
    /// Create a new Redshift collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            redshift_client: RedshiftClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all Redshift clusters in the configured region
    pub async fn list_clusters(&self) -> Result<Vec<(RedshiftClusterId, String)>> {
        info!("Listing Redshift clusters...");
        
        let mut cluster_ids = Vec::new();
        let mut paginator = self.redshift_client.describe_clusters().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(clusters) = page.clusters {
                for cluster in clusters {
                    if let Some(cluster_identifier) = cluster.cluster_identifier {
                        // Use cluster identifier as the ID
                        cluster_ids.push((
                            RedshiftClusterId(cluster_identifier.clone()),
                            cluster_identifier,
                        ));
                    }
                }
            }
        }

        info!("Found {} Redshift clusters", cluster_ids.len());
        Ok(cluster_ids)
    }

    /// Collect CloudWatch metrics for a single Redshift cluster
    pub async fn collect_metrics(
        &self,
        cluster_id: &RedshiftClusterId,
        cluster_identifier: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<RedshiftMetrics> {
        let mut metrics = RedshiftMetrics {
            cluster_id: cluster_id.0.clone(),
            cluster_identifier: cluster_identifier.to_string(),
            timestamp: end_time,
            cpu_utilization: None,
            database_connections: None,
            wlm_queue_length: None,
            read_latency: None,
            write_latency: None,
            read_throughput: None,
            write_throughput: None,
            network_receive_throughput: None,
            network_transmit_throughput: None,
            health_status: None,
            maintenance_mode: None,
            percentage_of_disk_space_used: None,
        };

        // Collect common metrics
        metrics.cpu_utilization = self.get_metric_statistic("CPUUtilization", cluster_identifier, start_time, end_time, false).await?;
        metrics.database_connections = self.get_metric_statistic("DatabaseConnections", cluster_identifier, start_time, end_time, false).await?;
        metrics.wlm_queue_length = self.get_metric_statistic("WLMQueueLength", cluster_identifier, start_time, end_time, false).await?;
        
        // I/O metrics
        metrics.read_latency = self.get_metric_statistic("ReadLatency", cluster_identifier, start_time, end_time, false).await?;
        metrics.write_latency = self.get_metric_statistic("WriteLatency", cluster_identifier, start_time, end_time, false).await?;
        metrics.read_throughput = self.get_metric_statistic("ReadThroughput", cluster_identifier, start_time, end_time, false).await?;
        metrics.write_throughput = self.get_metric_statistic("WriteThroughput", cluster_identifier, start_time, end_time, false).await?;
        
        // Network metrics
        metrics.network_receive_throughput = self.get_metric_statistic("NetworkReceiveThroughput", cluster_identifier, start_time, end_time, false).await?;
        metrics.network_transmit_throughput = self.get_metric_statistic("NetworkTransmitThroughput", cluster_identifier, start_time, end_time, false).await?;
        
        // Storage and health metrics
        metrics.percentage_of_disk_space_used = self.get_metric_statistic("PercentageOfDiskSpaceUsed", cluster_identifier, start_time, end_time, false).await?;

        // HealthStatus and MaintenanceMode are typically retrieved from describe_clusters API
        // rather than CloudWatch, but we'll try to get them from CloudWatch if available
        // For now, we'll leave them as None and they can be populated from the cluster description if needed

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for Redshift
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        cluster_identifier: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("ClusterIdentifier")
            .value(cluster_identifier)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/Redshift")
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
                warn!("Failed to get Redshift metric {} for cluster {}: {}", metric_name, cluster_identifier, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple Redshift clusters
    pub async fn collect_metrics_batch(
        &self,
        clusters: &[(RedshiftClusterId, String)],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<RedshiftMetrics>> {
        let mut metrics = Vec::new();

        for (cluster_id, cluster_identifier) in clusters {
            match self.collect_metrics(cluster_id, cluster_identifier, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Redshift cluster {}: {}", cluster_id.0, e);
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

/// Convert Redshift metrics to Reiver format
pub fn redshift_metrics_to_reiver_format(
    metrics: &RedshiftMetrics,
    project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let base_tags = vec![
        format!("project_id:{}", project_id),
        format!("cluster_id:{}", metrics.cluster_id),
        format!("cluster_identifier:{}", metrics.cluster_identifier),
        "source:aws_cloudwatch".to_string(),
        "service:redshift".to_string(),
    ];

    let mut add_metric = |name: &str, value: Option<f64>, metric_type: &str| {
        if let Some(v) = value {
            reiver_metrics.push(ReiverMetric {
                name: format!("redshift.{}", name),
                value: v,
                r#type: metric_type.to_string(),
                timestamp: metrics.timestamp,
                tags: base_tags.clone(),
            });
        }
    };

    add_metric("cpu_utilization", metrics.cpu_utilization, "gauge");
    add_metric("database_connections", metrics.database_connections, "gauge");
    add_metric("wlm_queue_length", metrics.wlm_queue_length, "gauge");
    add_metric("read_latency", metrics.read_latency, "gauge");
    add_metric("write_latency", metrics.write_latency, "gauge");
    add_metric("read_throughput", metrics.read_throughput, "gauge");
    add_metric("write_throughput", metrics.write_throughput, "gauge");
    add_metric("network_receive_throughput", metrics.network_receive_throughput, "gauge");
    add_metric("network_transmit_throughput", metrics.network_transmit_throughput, "gauge");
    add_metric("percentage_of_disk_space_used", metrics.percentage_of_disk_space_used, "gauge");

    reiver_metrics
}
