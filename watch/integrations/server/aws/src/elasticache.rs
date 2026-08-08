//! ElastiCache integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect ElastiCache (Redis/Memcached) cluster metrics from AWS CloudWatch.
//! Metrics collected include:
//! - CPUUtilization
//! - NetworkBytesIn/NetworkBytesOut
//! - CacheHits/CacheMisses
//! - CurrConnections
//! - Evictions
//! - ReplicationLag (for Redis replication)
//! - ReplicationBytes (for Redis replication)
//! - EngineCPUUtilization (for Redis)

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_elasticache::Client as ElastiCacheClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// ElastiCache cluster identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElastiCacheClusterId(pub String);

/// ElastiCache metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct ElastiCacheMetrics {
    pub cluster_id: String,
    pub engine: Option<String>, // "redis" or "memcached"
    pub timestamp: DateTime<Utc>,
    pub cpu_utilization: Option<f64>,
    pub network_bytes_in: Option<f64>,
    pub network_bytes_out: Option<f64>,
    pub cache_hits: Option<f64>,
    pub cache_misses: Option<f64>,
    pub curr_connections: Option<f64>,
    pub evictions: Option<f64>,
    pub replication_lag: Option<f64>, // Redis only
    pub replication_bytes: Option<f64>, // Redis only
    pub engine_cpu_utilization: Option<f64>, // Redis only
}

/// ElastiCache metrics collector
pub struct ElastiCacheCollector {
    elasticache_client: ElastiCacheClient,
    cloudwatch_client: CloudWatchClient,
}

impl ElastiCacheCollector {
    /// Create a new ElastiCache collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            elasticache_client: ElastiCacheClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all ElastiCache clusters in the configured region
    pub async fn list_clusters(&self) -> Result<Vec<(ElastiCacheClusterId, Option<String>)>> {
        info!("Listing ElastiCache clusters...");
        
        let mut cluster_ids = Vec::new();
        let mut paginator = self.elasticache_client.describe_cache_clusters().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(clusters) = page.cache_clusters {
                for cluster in clusters {
                    if let Some(cluster_id) = cluster.cache_cluster_id {
                        // Extract engine type (e.g., "redis", "memcached")
                        let engine = cluster.engine
                            .as_ref()
                            .and_then(|e| e.split('-').next())
                            .map(|s| s.to_lowercase());
                        
                        cluster_ids.push((ElastiCacheClusterId(cluster_id), engine));
                    }
                }
            }
        }

        info!("Found {} ElastiCache clusters", cluster_ids.len());
        Ok(cluster_ids)
    }

    /// Collect CloudWatch metrics for a single ElastiCache cluster
    pub async fn collect_metrics(
        &self,
        cluster_id: &ElastiCacheClusterId,
        engine: Option<&str>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<ElastiCacheMetrics> {
        let mut metrics = ElastiCacheMetrics {
            cluster_id: cluster_id.0.clone(),
            engine: engine.map(|s| s.to_string()),
            timestamp: end_time,
            cpu_utilization: None,
            network_bytes_in: None,
            network_bytes_out: None,
            cache_hits: None,
            cache_misses: None,
            curr_connections: None,
            evictions: None,
            replication_lag: None,
            replication_bytes: None,
            engine_cpu_utilization: None,
        };

        // Collect common metrics
        metrics.cpu_utilization = self.get_metric_statistic("CPUUtilization", &cluster_id.0, start_time, end_time, false).await?;
        metrics.network_bytes_in = self.get_metric_statistic("NetworkBytesIn", &cluster_id.0, start_time, end_time, true).await?;
        metrics.network_bytes_out = self.get_metric_statistic("NetworkBytesOut", &cluster_id.0, start_time, end_time, true).await?;
        metrics.cache_hits = self.get_metric_statistic("CacheHits", &cluster_id.0, start_time, end_time, true).await?;
        metrics.cache_misses = self.get_metric_statistic("CacheMisses", &cluster_id.0, start_time, end_time, true).await?;
        metrics.curr_connections = self.get_metric_statistic("CurrConnections", &cluster_id.0, start_time, end_time, false).await?;
        metrics.evictions = self.get_metric_statistic("Evictions", &cluster_id.0, start_time, end_time, true).await?;
        
        // Redis-specific metrics
        if engine == Some("redis") {
            metrics.replication_lag = self.get_metric_statistic("ReplicationLag", &cluster_id.0, start_time, end_time, false).await?;
            metrics.replication_bytes = self.get_metric_statistic("ReplicationBytes", &cluster_id.0, start_time, end_time, true).await?;
            metrics.engine_cpu_utilization = self.get_metric_statistic("EngineCPUUtilization", &cluster_id.0, start_time, end_time, false).await?;
        }

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for ElastiCache
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        cluster_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("CacheClusterId")
            .value(cluster_id)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/ElastiCache")
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
                warn!("Failed to get ElastiCache metric {} for cluster {}: {}", metric_name, cluster_id, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple ElastiCache clusters
    pub async fn collect_metrics_batch(
        &self,
        clusters: &[(ElastiCacheClusterId, Option<String>)],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<ElastiCacheMetrics>> {
        let mut metrics = Vec::new();

        for (cluster_id, engine) in clusters {
            match self.collect_metrics(cluster_id, engine.as_deref(), start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for ElastiCache cluster {}: {}", cluster_id.0, e);
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

/// Convert ElastiCache metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn elasticache_metrics_to_reiver_format(
    metrics: &ElastiCacheMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let mut tags = vec![
        format!("cluster_id:{}", metrics.cluster_id),
        "source:aws_elasticache".to_string(),
    ];
    
    if let Some(engine) = &metrics.engine {
        tags.push(format!("engine:{}", engine));
    }

    if let Some(cpu) = metrics.cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.cpu_utilization".to_string(),
            value: cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bytes_in) = metrics.network_bytes_in {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.network_bytes_in".to_string(),
            value: bytes_in,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bytes_out) = metrics.network_bytes_out {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.network_bytes_out".to_string(),
            value: bytes_out,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(hits) = metrics.cache_hits {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.cache_hits".to_string(),
            value: hits,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(misses) = metrics.cache_misses {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.cache_misses".to_string(),
            value: misses,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(connections) = metrics.curr_connections {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.curr_connections".to_string(),
            value: connections,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(evictions) = metrics.evictions {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.evictions".to_string(),
            value: evictions,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(lag) = metrics.replication_lag {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.replication_lag".to_string(),
            value: lag,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bytes) = metrics.replication_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.replication_bytes".to_string(),
            value: bytes,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(engine_cpu) = metrics.engine_cpu_utilization {
        reiver_metrics.push(ReiverMetric {
            name: "aws.elasticache.engine_cpu_utilization".to_string(),
            value: engine_cpu,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

