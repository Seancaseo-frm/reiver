//! EC2 integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect EC2 instance metrics from AWS CloudWatch.
//! Metrics collected include:
//! - CPUUtilization
//! - NetworkIn/NetworkOut
//! - DiskReadOps/DiskWriteOps
//! - DiskReadBytes/DiskWriteBytes

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_ec2::Client as Ec2Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// EC2 instance identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ec2InstanceId(pub String);

/// EC2 metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct Ec2Metrics {
    pub instance_id: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub network_in_bytes: Option<f64>,
    pub network_out_bytes: Option<f64>,
    pub disk_read_ops: Option<f64>,
    pub disk_write_ops: Option<f64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub status_check_failed: Option<f64>,
    pub status_check_failed_instance: Option<f64>,
    pub status_check_failed_system: Option<f64>,
}

/// EC2 metrics collector
pub struct Ec2Collector {
    ec2_client: Ec2Client,
    cloudwatch_client: CloudWatchClient,
}

impl Ec2Collector {
    /// Create a new EC2 collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            ec2_client: Ec2Client::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all EC2 instances in the configured region
    pub async fn list_instances(&self) -> Result<Vec<Ec2InstanceId>> {
        let response = self.ec2_client
            .describe_instances()
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to describe EC2 instances: {}", e))?;

        let mut instance_ids = Vec::new();

        if let Some(reservations) = response.reservations {
            for reservation in reservations {
                if let Some(instances) = reservation.instances {
                    for instance in instances {
                        if let Some(instance_id) = instance.instance_id {
                            instance_ids.push(Ec2InstanceId(instance_id));
                        }
                    }
                }
            }
        }

        info!("Found {} EC2 instances", instance_ids.len());
        Ok(instance_ids)
    }

    /// Collect metrics for a specific EC2 instance
    pub async fn collect_metrics(
        &self,
        instance_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Ec2Metrics> {
        info!("Collecting EC2 metrics for instance: {}", instance_id);

        // Collect CPU utilization
        let cpu_utilization = self
            .get_metric_statistics(
                "AWS/EC2",
                "CPUUtilization",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        // Collect network metrics
        let network_in_bytes = self
            .get_metric_statistics(
                "AWS/EC2",
                "NetworkIn",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let network_out_bytes = self
            .get_metric_statistics(
                "AWS/EC2",
                "NetworkOut",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        // Collect disk metrics
        let disk_read_ops = self
            .get_metric_statistics(
                "AWS/EC2",
                "DiskReadOps",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let disk_write_ops = self
            .get_metric_statistics(
                "AWS/EC2",
                "DiskWriteOps",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let disk_read_bytes = self
            .get_metric_statistics(
                "AWS/EC2",
                "DiskReadBytes",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let disk_write_bytes = self
            .get_metric_statistics(
                "AWS/EC2",
                "DiskWriteBytes",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        // Collect status check metrics
        let status_check_failed = self
            .get_metric_statistics(
                "AWS/EC2",
                "StatusCheckFailed",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let status_check_failed_instance = self
            .get_metric_statistics(
                "AWS/EC2",
                "StatusCheckFailed_Instance",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        let status_check_failed_system = self
            .get_metric_statistics(
                "AWS/EC2",
                "StatusCheckFailed_System",
                instance_id,
                start_time,
                end_time,
            )
            .await?;

        Ok(Ec2Metrics {
            instance_id: instance_id.to_string(),
            timestamp: end_time,
            cpu_utilization,
            network_in_bytes,
            network_out_bytes,
            disk_read_ops,
            disk_write_ops,
            disk_read_bytes,
            disk_write_bytes,
            status_check_failed,
            status_check_failed_instance,
            status_check_failed_system,
        })
    }

    /// Get metric statistics from CloudWatch
    async fn get_metric_statistics(
        &self,
        namespace: &str,
        metric_name: &str,
        instance_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let response = self
            .cloudwatch_client
            .get_metric_statistics()
            .namespace(namespace)
            .metric_name(metric_name)
            .dimensions(
                aws_sdk_cloudwatch::types::Dimension::builder()
                    .name("InstanceId")
                    .value(instance_id)
                    .build(),
            )
            .start_time(start_aws)
            .end_time(end_aws)
            .period(300) // 5-minute periods
            .statistics(aws_sdk_cloudwatch::types::Statistic::Average)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if let Some(datapoints) = resp.datapoints {
                    // Get the most recent datapoint
                    if let Some(latest) = datapoints.iter().max_by_key(|dp| {
                        dp.timestamp().map(|t| t.secs()).unwrap_or(0)
                    }) {
                        if let Some(value) = latest.average {
                            return Ok(Some(value));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!(
                    "Failed to get metric {} for instance {}: {}",
                    metric_name, instance_id, e
                );
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect metrics for multiple instances
    pub async fn collect_metrics_batch(
        &self,
        instance_ids: &[Ec2InstanceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<Ec2Metrics>> {
        let mut metrics = Vec::new();

        for instance_id in instance_ids {
            match self
                .collect_metrics(&instance_id.0, start_time, end_time)
                .await
            {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!(
                        "Failed to collect metrics for instance {}: {}",
                        instance_id.0, e
                    );
                }
            }
        }

        Ok(metrics)
    }
}

/// Convert EC2 metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn ec2_metrics_to_reiver_format(
    metrics: &Ec2Metrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("instance_id:{}", metrics.instance_id),
        "source:aws_ec2".to_string(),
    ];

    if let Some(cpu) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.cpu_utilization".to_string(),
            value: cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(net_in) = metrics.network_in_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.network_in_bytes".to_string(),
            value: net_in,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(net_out) = metrics.network_out_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.network_out_bytes".to_string(),
            value: net_out,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(read_ops) = metrics.disk_read_ops {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.disk_read_ops".to_string(),
            value: read_ops,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(write_ops) = metrics.disk_write_ops {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.disk_write_ops".to_string(),
            value: write_ops,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(read_bytes) = metrics.disk_read_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.disk_read_bytes".to_string(),
            value: read_bytes,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(write_bytes) = metrics.disk_write_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.disk_write_bytes".to_string(),
            value: write_bytes,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(status_failed) = metrics.status_check_failed {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.status_check_failed".to_string(),
            value: status_failed,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(status_failed_instance) = metrics.status_check_failed_instance {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.status_check_failed_instance".to_string(),
            value: status_failed_instance,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(status_failed_system) = metrics.status_check_failed_system {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ec2.status_check_failed_system".to_string(),
            value: status_failed_system,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
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

