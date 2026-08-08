//! S3 integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect S3 bucket metrics from AWS CloudWatch.
//! Metrics collected include:
//! - BucketSizeBytes
//! - NumberOfObjects
//! - AllRequests
//! - GetRequests
//! - PutRequests
//! - DeleteRequests
//! - HeadRequests
//! - ListRequests
//! - BytesDownloaded
//! - BytesUploaded
//! - 4xxErrors
//! - 5xxErrors

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::AwsConfig;

/// S3 bucket identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BucketName(pub String);

/// S3 metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct S3Metrics {
    pub bucket_name: String,
    pub timestamp: DateTime<Utc>,
    pub bucket_size_bytes: Option<f64>,
    pub number_of_objects: Option<f64>,
    pub all_requests: Option<f64>,
    pub get_requests: Option<f64>,
    pub put_requests: Option<f64>,
    pub delete_requests: Option<f64>,
    pub head_requests: Option<f64>,
    pub list_requests: Option<f64>,
    pub bytes_downloaded: Option<f64>,
    pub bytes_uploaded: Option<f64>,
    pub errors_4xx: Option<f64>,
    pub errors_5xx: Option<f64>,
}

/// S3 metrics collector
pub struct S3Collector {
    s3_client: S3Client,
    cloudwatch_client: CloudWatchClient,
}

impl S3Collector {
    /// Create a new S3 collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            s3_client: S3Client::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all S3 buckets in the configured region (S3 is global, but metrics are regional)
    pub async fn list_buckets(&self) -> Result<Vec<S3BucketName>> {
        info!("Listing S3 buckets...");
        
        let response = self.s3_client
            .list_buckets()
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list S3 buckets: {}", e))?;

        let mut bucket_names = Vec::new();
        
        if let Some(buckets) = response.buckets {
            for bucket in buckets {
                if let Some(name) = bucket.name {
                    bucket_names.push(S3BucketName(name));
                }
            }
        }

        info!("Found {} S3 buckets", bucket_names.len());
        Ok(bucket_names)
    }

    /// Collect CloudWatch metrics for a single S3 bucket
    pub async fn collect_metrics(
        &self,
        bucket_name: &S3BucketName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<S3Metrics> {
        let mut metrics = S3Metrics {
            bucket_name: bucket_name.0.clone(),
            timestamp: end_time,
            bucket_size_bytes: None,
            number_of_objects: None,
            all_requests: None,
            get_requests: None,
            put_requests: None,
            delete_requests: None,
            head_requests: None,
            list_requests: None,
            bytes_downloaded: None,
            bytes_uploaded: None,
            errors_4xx: None,
            errors_5xx: None,
        };

        // Collect metrics using helper method
        metrics.bucket_size_bytes = self.get_metric_statistics("BucketSizeBytes", &bucket_name.0, start_time, end_time, false).await?;
        metrics.number_of_objects = self.get_metric_statistics("NumberOfObjects", &bucket_name.0, start_time, end_time, false).await?;
        metrics.all_requests = self.get_metric_statistics("AllRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.get_requests = self.get_metric_statistics("GetRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.put_requests = self.get_metric_statistics("PutRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.delete_requests = self.get_metric_statistics("DeleteRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.head_requests = self.get_metric_statistics("HeadRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.list_requests = self.get_metric_statistics("ListRequests", &bucket_name.0, start_time, end_time, true).await?;
        metrics.bytes_downloaded = self.get_metric_statistics("BytesDownloaded", &bucket_name.0, start_time, end_time, true).await?;
        metrics.bytes_uploaded = self.get_metric_statistics("BytesUploaded", &bucket_name.0, start_time, end_time, true).await?;
        metrics.errors_4xx = self.get_metric_statistics("4xxErrors", &bucket_name.0, start_time, end_time, true).await?;
        metrics.errors_5xx = self.get_metric_statistics("5xxErrors", &bucket_name.0, start_time, end_time, true).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for S3
    async fn get_metric_statistics(
        &self,
        metric_name: &str,
        bucket_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("BucketName")
            .value(bucket_name)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/S3")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(86400); // S3 metrics are reported daily, use 1 day period

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
                tracing::warn!("Failed to get S3 metric {} for bucket {}: {}", metric_name, bucket_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple S3 buckets
    pub async fn collect_metrics_batch(
        &self,
        bucket_names: &[S3BucketName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<S3Metrics>> {
        let mut metrics = Vec::new();

        for bucket_name in bucket_names {
            match self.collect_metrics(bucket_name, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for bucket {}: {}", bucket_name.0, e);
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

/// Convert S3 metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn s3_metrics_to_reiver_format(
    metrics: &S3Metrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("bucket_name:{}", metrics.bucket_name),
        "source:aws_s3".to_string(),
    ];

    if let Some(size) = metrics.bucket_size_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.bucket_size_bytes".to_string(),
            value: size,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(count) = metrics.number_of_objects {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.number_of_objects".to_string(),
            value: count,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(all_reqs) = metrics.all_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.all_requests".to_string(),
            value: all_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(get_reqs) = metrics.get_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.get_requests".to_string(),
            value: get_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(put_reqs) = metrics.put_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.put_requests".to_string(),
            value: put_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(delete_reqs) = metrics.delete_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.delete_requests".to_string(),
            value: delete_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(head_reqs) = metrics.head_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.head_requests".to_string(),
            value: head_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(list_reqs) = metrics.list_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.list_requests".to_string(),
            value: list_reqs,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bytes_down) = metrics.bytes_downloaded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.bytes_downloaded".to_string(),
            value: bytes_down,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(bytes_up) = metrics.bytes_uploaded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.bytes_uploaded".to_string(),
            value: bytes_up,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(errors_4xx) = metrics.errors_4xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.errors_4xx".to_string(),
            value: errors_4xx,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(errors_5xx) = metrics.errors_5xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.s3.errors_5xx".to_string(),
            value: errors_5xx,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

