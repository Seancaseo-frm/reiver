//! Kinesis integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect Kinesis stream metrics from AWS CloudWatch.
//! Metrics collected include:
//! - GetRecords
//! - PutRecords
//! - IncomingRecords
//! - OutgoingRecords
//! - IteratorAge
//! - ReadProvisionedThroughputExceeded
//! - WriteProvisionedThroughputExceeded

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_kinesis::Client as KinesisClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// Kinesis stream identifier (stream name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisStreamName(pub String);

/// Kinesis stream metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct KinesisMetrics {
    pub stream_name: String,
    pub timestamp: DateTime<Utc>,
    pub get_records: Option<f64>,
    pub put_records: Option<f64>,
    pub incoming_records: Option<f64>,
    pub outgoing_records: Option<f64>,
    pub iterator_age: Option<f64>,
    pub read_provisioned_throughput_exceeded: Option<f64>,
    pub write_provisioned_throughput_exceeded: Option<f64>,
}

/// Kinesis metrics collector
pub struct KinesisCollector {
    kinesis_client: KinesisClient,
    cloudwatch_client: CloudWatchClient,
}

impl KinesisCollector {
    /// Create a new Kinesis collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            kinesis_client: KinesisClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all Kinesis streams in the configured region
    pub async fn list_streams(&self) -> Result<Vec<KinesisStreamName>> {
        info!("Listing Kinesis streams...");
        
        let mut stream_names = Vec::new();
        let mut paginator = self.kinesis_client.list_streams().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            for stream_name in page.stream_names {
                stream_names.push(KinesisStreamName(stream_name));
            }
        }

        info!("Found {} Kinesis streams", stream_names.len());
        Ok(stream_names)
    }

    /// Collect CloudWatch metrics for a single Kinesis stream
    pub async fn collect_metrics(
        &self,
        stream_name: &KinesisStreamName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<KinesisMetrics> {
        let mut metrics = KinesisMetrics {
            stream_name: stream_name.0.clone(),
            timestamp: end_time,
            get_records: None,
            put_records: None,
            incoming_records: None,
            outgoing_records: None,
            iterator_age: None,
            read_provisioned_throughput_exceeded: None,
            write_provisioned_throughput_exceeded: None,
        };

        // Collect metrics using helper method
        metrics.get_records = self.get_cloudwatch_metric_statistic("GetRecords", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.put_records = self.get_cloudwatch_metric_statistic("PutRecords", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.incoming_records = self.get_cloudwatch_metric_statistic("IncomingRecords", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.outgoing_records = self.get_cloudwatch_metric_statistic("OutgoingRecords", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.iterator_age = self.get_cloudwatch_metric_statistic("IteratorAge", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Maximum).await?;
        metrics.read_provisioned_throughput_exceeded = self.get_cloudwatch_metric_statistic("ReadProvisionedThroughputExceeded", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.write_provisioned_throughput_exceeded = self.get_cloudwatch_metric_statistic("WriteProvisionedThroughputExceeded", &stream_name.0, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for Kinesis
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        stream_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("StreamName")
            .value(stream_name)
            .build();

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/Kinesis")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60) // 1-minute periods for Kinesis
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
                warn!("Failed to get Kinesis metric {} for stream {}: {}", metric_name, stream_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple Kinesis streams in parallel
    pub async fn collect_metrics_batch(
        &self,
        streams: &[KinesisStreamName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<KinesisMetrics>> {
        let mut tasks = Vec::new();
        for stream_name in streams {
            let collector = self.clone();
            let stream_name_clone = stream_name.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&stream_name_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Kinesis stream: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for KinesisCollector {
    fn clone(&self) -> Self {
        Self {
            kinesis_client: self.kinesis_client.clone(),
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

/// Convert Kinesis metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn kinesis_metrics_to_reiver_format(
    metrics: &KinesisMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("stream_name:{}", metrics.stream_name),
        "source:aws_kinesis".to_string(),
    ];

    if let Some(value) = metrics.get_records {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.get_records".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.put_records {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.put_records".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.incoming_records {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.incoming_records".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.outgoing_records {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.outgoing_records".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.iterator_age {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.iterator_age".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.read_provisioned_throughput_exceeded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.read_provisioned_throughput_exceeded".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.write_provisioned_throughput_exceeded {
        reiver_metrics.push(ReiverMetric {
            name: "aws.kinesis.write_provisioned_throughput_exceeded".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

