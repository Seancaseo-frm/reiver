//! CloudFront integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect CloudFront distribution metrics from AWS CloudWatch.
//! Metrics collected include:
//! - Requests (total number of requests)
//! - BytesDownloaded
//! - BytesUploaded
//! - 4xxErrorRate
//! - 5xxErrorRate
//! - TotalErrorRate
//! - CacheHitRate
//! - CacheMissRate

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_cloudfront::Client as CloudFrontClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// CloudFront distribution identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFrontDistributionId(pub String);

/// CloudFront distribution metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct CloudFrontMetrics {
    pub distribution_id: String,
    pub timestamp: DateTime<Utc>,
    pub requests: Option<f64>,
    pub bytes_downloaded: Option<f64>,
    pub bytes_uploaded: Option<f64>,
    pub error_rate_4xx: Option<f64>,
    pub error_rate_5xx: Option<f64>,
    pub total_error_rate: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    pub cache_miss_rate: Option<f64>,
}

/// CloudFront metrics collector
pub struct CloudFrontCollector {
    cloudfront_client: CloudFrontClient,
    cloudwatch_client: CloudWatchClient,
}

impl CloudFrontCollector {
    /// Create a new CloudFront collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and default credential chain
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            cloudfront_client: CloudFrontClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all CloudFront distributions in the configured region
    pub async fn list_distributions(&self) -> Result<Vec<CloudFrontDistributionId>> {
        info!("Listing CloudFront distributions...");
        
        let mut distribution_ids = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut request = self.cloudfront_client.list_distributions();
            if let Some(ref marker_value) = marker {
                request = request.marker(marker_value);
            }

            let response = request.send().await?;
            
            if let Some(dist_list) = response.distribution_list {
                // Extract distribution IDs from items
                if let Some(items) = dist_list.items {
                    for item in items {
                        distribution_ids.push(CloudFrontDistributionId(item.id));
                    }
                }

                // Check if there are more distributions to fetch
                if dist_list.is_truncated {
                    marker = dist_list.next_marker;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        info!("Found {} CloudFront distributions", distribution_ids.len());
        Ok(distribution_ids)
    }

    /// Collect CloudWatch metrics for a single CloudFront distribution
    pub async fn collect_metrics(
        &self,
        distribution_id: &CloudFrontDistributionId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<CloudFrontMetrics> {
        let mut metrics = CloudFrontMetrics {
            distribution_id: distribution_id.0.clone(),
            timestamp: end_time,
            requests: None,
            bytes_downloaded: None,
            bytes_uploaded: None,
            error_rate_4xx: None,
            error_rate_5xx: None,
            total_error_rate: None,
            cache_hit_rate: None,
            cache_miss_rate: None,
        };

        // Collect metrics using helper method
        // CloudFront metrics use Dimension: DistributionId
        metrics.requests = self.get_cloudwatch_metric_statistic("Requests", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.bytes_downloaded = self.get_cloudwatch_metric_statistic("BytesDownloaded", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.bytes_uploaded = self.get_cloudwatch_metric_statistic("BytesUploaded", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.error_rate_4xx = self.get_cloudwatch_metric_statistic("4xxErrorRate", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.error_rate_5xx = self.get_cloudwatch_metric_statistic("5xxErrorRate", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.total_error_rate = self.get_cloudwatch_metric_statistic("TotalErrorRate", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.cache_hit_rate = self.get_cloudwatch_metric_statistic("CacheHitRate", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.cache_miss_rate = self.get_cloudwatch_metric_statistic("CacheMissRate", &distribution_id.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for CloudFront
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        distribution_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("DistributionId")
            .value(distribution_id)
            .build();

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/CloudFront")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(300) // 5-minute periods for CloudFront
            .statistics(statistic.clone())
            .send()
            .await;

        match response {
            Ok(resp) => {
                if let Some(datapoints) = resp.datapoints {
                    if let Some(latest) = datapoints.iter().max_by_key(|dp| {
                        dp.timestamp().map(|t| t.secs()).unwrap_or(0)
                    }) {
                        let value = match statistic {
                            aws_sdk_cloudwatch::types::Statistic::Average => latest.average,
                            aws_sdk_cloudwatch::types::Statistic::Sum => latest.sum,
                            aws_sdk_cloudwatch::types::Statistic::Maximum => latest.maximum,
                            aws_sdk_cloudwatch::types::Statistic::Minimum => latest.minimum,
                            aws_sdk_cloudwatch::types::Statistic::SampleCount => latest.sample_count,
                            _ => None,
                        };
                        Ok(value)
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                warn!("Failed to get CloudFront metric {} for distribution {}: {}", metric_name, distribution_id, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple CloudFront distributions in parallel
    pub async fn collect_metrics_batch(
        &self,
        distributions: &[CloudFrontDistributionId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<CloudFrontMetrics>> {
        let mut tasks = Vec::new();
        for distribution_id in distributions {
            let collector = self.clone();
            let distribution_id_clone = distribution_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&distribution_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for CloudFront distribution: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for CloudFrontCollector {
    fn clone(&self) -> Self {
        Self {
            cloudfront_client: self.cloudfront_client.clone(),
            cloudwatch_client: self.cloudwatch_client.clone(),
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

/// Convert CloudFront metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn cloudfront_metrics_to_reiver_format(
    metrics: &CloudFrontMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("distribution_id:{}", metrics.distribution_id),
        "source:aws_cloudfront".to_string(),
    ];

    if let Some(value) = metrics.requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.requests".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_downloaded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.bytes_downloaded".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.bytes_uploaded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.bytes_uploaded".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.error_rate_4xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.error_rate_4xx".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.error_rate_5xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.error_rate_5xx".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.total_error_rate {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.total_error_rate".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_hit_rate {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.cache_hit_rate".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_miss_rate {
        reiver_metrics.push(ReiverMetric {
            name: "aws.cloudfront.cache_miss_rate".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

