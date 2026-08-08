//! RDS integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect RDS instance metrics from AWS CloudWatch.
//! Metrics collected include:
//! - CPUUtilization
//! - DatabaseConnections
//! - FreeableMemory
//! - FreeStorageSpace
//! - ReadLatency/WriteLatency
//! - ReadIOPS/WriteIOPS
//! - ReadThroughput/WriteThroughput
//! - NetworkReceiveThroughput/NetworkTransmitThroughput
//! - BinLogDiskUsage (for MySQL/MariaDB)
//! - ReplicaLag (for read replicas)

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_rds::Client as RdsClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// RDS instance identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdsInstanceId(pub String);

/// RDS metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct RdsMetrics {
    pub instance_id: String,
    pub engine: Option<String>, // e.g., "postgres", "mysql", "mariadb"
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub database_connections: Option<f64>,
    pub freeable_memory: Option<f64>,
    pub free_storage_space: Option<f64>,
    pub read_latency: Option<f64>,
    pub write_latency: Option<f64>,
    pub read_iops: Option<f64>,
    pub write_iops: Option<f64>,
    pub read_throughput: Option<f64>,
    pub write_throughput: Option<f64>,
    pub network_receive_throughput: Option<f64>,
    pub network_transmit_throughput: Option<f64>,
    pub bin_log_disk_usage: Option<f64>, // MySQL/MariaDB only
    pub replica_lag: Option<f64>, // For read replicas
}

/// RDS metrics collector
pub struct RdsCollector {
    rds_client: RdsClient,
    cloudwatch_client: CloudWatchClient,
}

impl RdsCollector {
    /// Create a new RDS collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            rds_client: RdsClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all RDS DB instances in the configured region
    pub async fn list_instances(&self) -> Result<Vec<(RdsInstanceId, Option<String>)>> {
        info!("Listing RDS instances...");
        
        let mut instance_ids = Vec::new();
        let mut paginator = self.rds_client.describe_db_instances().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(db_instances) = page.db_instances {
                for instance in db_instances {
                    if let Some(db_instance_identifier) = instance.db_instance_identifier {
                        // Extract engine type (e.g., "postgres", "mysql", "mariadb")
                        let engine = instance.engine
                            .and_then(|e| e.split('-').
                                next().
                                map(|s| s.to_lowercase()));
                        
                        instance_ids.push((RdsInstanceId(db_instance_identifier), engine));
                    }
                }
            }
        }

        info!("Found {} RDS instances", instance_ids.len());
        Ok(instance_ids)
    }

    /// Collect CloudWatch metrics for a single RDS instance
    pub async fn collect_metrics(
        &self,
        instance_id: &RdsInstanceId,
        engine: Option<&str>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<RdsMetrics> {
        let mut metrics = RdsMetrics {
            instance_id: instance_id.0.clone(),
            engine: engine.map(|s| s.to_string()),
            timestamp: end_time,
            cpu_utilization: None,
            database_connections: None,
            freeable_memory: None,
            free_storage_space: None,
            read_latency: None,
            write_latency: None,
            read_iops: None,
            write_iops: None,
            read_throughput: None,
            write_throughput: None,
            network_receive_throughput: None,
            network_transmit_throughput: None,
            bin_log_disk_usage: None,
            replica_lag: None,
        };

        // Collect common metrics
        metrics.cpu_utilization = self.get_metric_statistic("CPUUtilization", &instance_id.0, start_time, end_time, false).await?;
        metrics.database_connections = self.get_metric_statistic("DatabaseConnections", &instance_id.0, start_time, end_time, false).await?;
        metrics.freeable_memory = self.get_metric_statistic("FreeableMemory", &instance_id.0, start_time, end_time, false).await?;
        metrics.free_storage_space = self.get_metric_statistic("FreeStorageSpace", &instance_id.0, start_time, end_time, false).await?;
        
        // I/O metrics
        metrics.read_latency = self.get_metric_statistic("ReadLatency", &instance_id.0, start_time, end_time, false).await?;
        metrics.write_latency = self.get_metric_statistic("WriteLatency", &instance_id.0, start_time, end_time, false).await?;
        metrics.read_iops = self.get_metric_statistic("ReadIOPS", &instance_id.0, start_time, end_time, false).await?;
        metrics.write_iops = self.get_metric_statistic("WriteIOPS", &instance_id.0, start_time, end_time, false).await?;
        metrics.read_throughput = self.get_metric_statistic("ReadThroughput", &instance_id.0, start_time, end_time, false).await?;
        metrics.write_throughput = self.get_metric_statistic("WriteThroughput", &instance_id.0, start_time, end_time, false).await?;
        
        // Network metrics
        metrics.network_receive_throughput = self.get_metric_statistic("NetworkReceiveThroughput", &instance_id.0, start_time, end_time, false).await?;
        metrics.network_transmit_throughput = self.get_metric_statistic("NetworkTransmitThroughput", &instance_id.0, start_time, end_time, false).await?;
        
        // MySQL/MariaDB specific metrics
        if engine == Some("mysql") || engine == Some("mariadb") {
            metrics.bin_log_disk_usage = self.get_metric_statistic("BinLogDiskUsage", &instance_id.0, start_time, end_time, false).await?;
        }
        
        // Replica lag (for read replicas)
        metrics.replica_lag = self.get_metric_statistic("ReplicaLag", &instance_id.0, start_time, end_time, false).await?;

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for RDS
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        instance_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("DBInstanceIdentifier")
            .value(instance_id)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/RDS")
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
                warn!("Failed to get RDS metric {} for instance {}: {}", metric_name, instance_id, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple RDS instances
    pub async fn collect_metrics_batch(
        &self,
        instances: &[(RdsInstanceId, Option<String>)],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<RdsMetrics>> {
        let mut metrics = Vec::new();

        for (instance_id, engine) in instances {
            match self.collect_metrics(instance_id, engine.as_deref(), start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for RDS instance {}: {}", instance_id.0, e);
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

/// Convert RDS metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn rds_metrics_to_reiver_format(
    metrics: &RdsMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let mut tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        "source:aws_rds".to_string(),
    ];
    
    if let Some(engine) = &metrics.engine {
        tags.push(format!("engine:{}", engine));
    }

    if let Some(cpu) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.cpu_utilization".to_string(),
            value: cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(connections) = metrics.database_connections {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.database_connections".to_string(),
            value: connections,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(memory) = metrics.freeable_memory {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.freeable_memory".to_string(),
            value: memory,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(storage) = metrics.free_storage_space {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.free_storage_space".to_string(),
            value: storage,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(latency) = metrics.read_latency {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.read_latency".to_string(),
            value: latency,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(latency) = metrics.write_latency {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.write_latency".to_string(),
            value: latency,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(iops) = metrics.read_iops {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.read_iops".to_string(),
            value: iops,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(iops) = metrics.write_iops {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.write_iops".to_string(),
            value: iops,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(throughput) = metrics.read_throughput {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.read_throughput".to_string(),
            value: throughput,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(throughput) = metrics.write_throughput {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.write_throughput".to_string(),
            value: throughput,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(throughput) = metrics.network_receive_throughput {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.network_receive_throughput".to_string(),
            value: throughput,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(throughput) = metrics.network_transmit_throughput {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.network_transmit_throughput".to_string(),
            value: throughput,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bin_log) = metrics.bin_log_disk_usage {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.bin_log_disk_usage".to_string(),
            value: bin_log,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(lag) = metrics.replica_lag {
        reiver_metrics.push(ReiverMetric {
            name: "aws.rds.replica_lag".to_string(),
            value: lag,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

