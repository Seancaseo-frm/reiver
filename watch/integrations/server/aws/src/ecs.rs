//! ECS integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect ECS cluster and service metrics from AWS CloudWatch.
//! Metrics collected include:
//! - CPUUtilization (cluster/service level)
//! - MemoryUtilization (cluster/service level)
//! - CPUReservation (cluster level)
//! - MemoryReservation (cluster level)
//! - RunningTaskCount
//! - PendingTaskCount
//! - DesiredTaskCount (service level)

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_ecs::Client as EcsClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// ECS cluster identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcsClusterName(pub String);

/// ECS service identifier (cluster name + service name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcsServiceId {
    pub cluster_name: String,
    pub service_name: String,
}

/// ECS metrics collected from CloudWatch (cluster level)
#[derive(Debug, Clone, Serialize)]
pub struct EcsClusterMetrics {
    pub cluster_name: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub cpu_reservation: Option<f64>,
    pub memory_reservation: Option<f64>,
    pub active_container_instances: Option<f64>,
    pub running_task_count: Option<f64>,
    pub pending_task_count: Option<f64>,
}

/// ECS metrics collected from CloudWatch (service level)
#[derive(Debug, Clone, Serialize)]
pub struct EcsServiceMetrics {
    pub cluster_name: String,
    pub service_name: String,
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub memory_utilization: Option<f64>,
    pub running_task_count: Option<f64>,
    pub pending_task_count: Option<f64>,
    pub desired_task_count: Option<f64>,
}

/// ECS metrics collector
pub struct EcsCollector {
    ecs_client: EcsClient,
    cloudwatch_client: CloudWatchClient,
}

impl EcsCollector {
    /// Create a new ECS collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            ecs_client: EcsClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all ECS clusters in the configured region
    pub async fn list_clusters(&self) -> Result<Vec<EcsClusterName>> {
        info!("Listing ECS clusters...");
        
        let mut cluster_names = Vec::new();
        let mut paginator = self.ecs_client.list_clusters().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(cluster_arns) = page.cluster_arns {
                for cluster_arn in cluster_arns {
                    // Extract cluster name from ARN (format: arn:aws:ecs:region:account:cluster/cluster-name)
                    if let Some(name) = cluster_arn.split('/').last() {
                        cluster_names.push(EcsClusterName(name.to_string()));
                    }
                }
            }
        }

        info!("Found {} ECS clusters", cluster_names.len());
        Ok(cluster_names)
    }

    /// List all services in a cluster
    pub async fn list_services(&self, cluster_name: &str) -> Result<Vec<EcsServiceId>> {
        let mut service_ids = Vec::new();
        let mut paginator = self.ecs_client
            .list_services()
            .cluster(cluster_name)
            .into_paginator()
            .send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(service_arns) = page.service_arns {
                for service_arn in service_arns {
                    // Extract service name from ARN
                    if let Some(service_name) = service_arn.split('/').last() {
                        service_ids.push(EcsServiceId {
                            cluster_name: cluster_name.to_string(),
                            service_name: service_name.to_string(),
                        });
                    }
                }
            }
        }

        Ok(service_ids)
    }

    /// Collect CloudWatch metrics for an ECS cluster
    pub async fn collect_cluster_metrics(
        &self,
        cluster_name: &EcsClusterName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<EcsClusterMetrics> {
        let mut metrics = EcsClusterMetrics {
            cluster_name: cluster_name.0.clone(),
            timestamp: end_time,
            cpu_utilization: None,
            memory_utilization: None,
            cpu_reservation: None,
            memory_reservation: None,
            active_container_instances: None,
            running_task_count: None,
            pending_task_count: None,
        };

        // Collect cluster-level metrics
        metrics.cpu_utilization = self.get_metric_statistic("CPUUtilization", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.memory_utilization = self.get_metric_statistic("MemoryUtilization", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.cpu_reservation = self.get_metric_statistic("CPUReservation", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.memory_reservation = self.get_metric_statistic("MemoryReservation", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.active_container_instances = self.get_metric_statistic("ActiveContainerInstances", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.running_task_count = self.get_metric_statistic("RunningTaskCount", &cluster_name.0, None, start_time, end_time, false).await?;
        metrics.pending_task_count = self.get_metric_statistic("PendingTaskCount", &cluster_name.0, None, start_time, end_time, false).await?;

        Ok(metrics)
    }

    /// Collect CloudWatch metrics for an ECS service
    pub async fn collect_service_metrics(
        &self,
        service_id: &EcsServiceId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<EcsServiceMetrics> {
        let mut metrics = EcsServiceMetrics {
            cluster_name: service_id.cluster_name.clone(),
            service_name: service_id.service_name.clone(),
            timestamp: end_time,
            cpu_utilization: None,
            memory_utilization: None,
            running_task_count: None,
            pending_task_count: None,
            desired_task_count: None,
        };

        // Collect service-level metrics
        metrics.cpu_utilization = self.get_metric_statistic("CPUUtilization", &service_id.cluster_name, Some(&service_id.service_name), start_time, end_time, false).await?;
        metrics.memory_utilization = self.get_metric_statistic("MemoryUtilization", &service_id.cluster_name, Some(&service_id.service_name), start_time, end_time, false).await?;
        metrics.running_task_count = self.get_metric_statistic("RunningTaskCount", &service_id.cluster_name, Some(&service_id.service_name), start_time, end_time, false).await?;
        metrics.pending_task_count = self.get_metric_statistic("PendingTaskCount", &service_id.cluster_name, Some(&service_id.service_name), start_time, end_time, false).await?;
        metrics.desired_task_count = self.get_metric_statistic("DesiredTaskCount", &service_id.cluster_name, Some(&service_id.service_name), start_time, end_time, false).await?;

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for ECS
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        cluster_name: &str,
        service_name: Option<&str>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let cluster_dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("ClusterName")
            .value(cluster_name)
            .build();

        let mut dimensions = vec![cluster_dimension];

        // Add service dimension if provided
        if let Some(service) = service_name {
            let service_dimension = aws_sdk_cloudwatch::types::Dimension::builder()
                .name("ServiceName")
                .value(service)
                .build();
            dimensions.push(service_dimension);
        }

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/ECS")
            .metric_name(metric_name)
            .set_dimensions(Some(dimensions))
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
                warn!("Failed to get ECS metric {} for cluster {}: {}", metric_name, cluster_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple ECS clusters
    pub async fn collect_cluster_metrics_batch(
        &self,
        clusters: &[EcsClusterName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<EcsClusterMetrics>> {
        let mut metrics = Vec::new();

        for cluster in clusters {
            match self.collect_cluster_metrics(cluster, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for ECS cluster {}: {}", cluster.0, e);
                }
            }
        }

        Ok(metrics)
    }

    /// Collect CloudWatch metrics for multiple ECS services
    pub async fn collect_service_metrics_batch(
        &self,
        services: &[EcsServiceId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<EcsServiceMetrics>> {
        let mut metrics = Vec::new();

        for service in services {
            match self.collect_service_metrics(service, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for ECS service {}: {}", service.service_name, e);
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

/// Convert ECS cluster metrics to Reiver metric format
pub fn ecs_cluster_metrics_to_reiver_format(
    metrics: &EcsClusterMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_name:{}", metrics.cluster_name),
        "source:aws_ecs".to_string(),
        "level:cluster".to_string(),
    ];

    if let Some(cpu) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.cpu_utilization".to_string(),
            value: cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(memory) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.memory_utilization".to_string(),
            value: memory,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(cpu_res) = metrics.cpu_reservation {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.cpu_reservation".to_string(),
            value: cpu_res,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(mem_res) = metrics.memory_reservation {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.memory_reservation".to_string(),
            value: mem_res,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(instances) = metrics.active_container_instances {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.active_container_instances".to_string(),
            value: instances,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(running) = metrics.running_task_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.running_task_count".to_string(),
            value: running,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(pending) = metrics.pending_task_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.cluster.pending_task_count".to_string(),
            value: pending,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

/// Convert ECS service metrics to Reiver metric format
pub fn ecs_service_metrics_to_reiver_format(
    metrics: &EcsServiceMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("cluster_name:{}", metrics.cluster_name),
        format!("service_name:{}", metrics.service_name),
        "source:aws_ecs".to_string(),
        "level:service".to_string(),
    ];

    if let Some(cpu) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.service.cpu_utilization".to_string(),
            value: cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(memory) = metrics.memory_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.service.memory_utilization".to_string(),
            value: memory,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(running) = metrics.running_task_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.service.running_task_count".to_string(),
            value: running,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(pending) = metrics.pending_task_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.service.pending_task_count".to_string(),
            value: pending,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(desired) = metrics.desired_task_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.ecs.service.desired_task_count".to_string(),
            value: desired,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

