//! SQS integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect SQS queue metrics from AWS CloudWatch.
//! Metrics collected include:
//! - NumberOfMessagesSent
//! - NumberOfMessagesReceived
//! - NumberOfMessagesDeleted
//! - ApproximateNumberOfMessagesVisible
//! - ApproximateNumberOfMessagesDelayed
//! - ApproximateNumberOfMessagesNotVisible
//! - SentMessageSize
//! - NumberOfEmptyReceives
//! - NumberOfMessagesReturned
//! - ApproximateAgeOfOldestMessage

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_sqs::Client as SqsClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// SQS queue identifier (queue URL)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsQueueUrl(pub String);

/// SQS queue metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct SqsMetrics {
    pub queue_url: String,
    pub queue_name: String,
    pub timestamp: DateTime<Utc>,
    pub messages_sent: Option<f64>,
    pub messages_received: Option<f64>,
    pub messages_deleted: Option<f64>,
    pub approximate_messages_visible: Option<f64>,
    pub approximate_messages_delayed: Option<f64>,
    pub approximate_messages_not_visible: Option<f64>,
    pub sent_message_size: Option<f64>,
    pub number_of_empty_receives: Option<f64>,
    pub number_of_messages_returned: Option<f64>,
    pub approximate_age_of_oldest_message: Option<f64>,
}

/// SQS metrics collector
pub struct SqsCollector {
    sqs_client: SqsClient,
    cloudwatch_client: CloudWatchClient,
}

impl SqsCollector {
    /// Create a new SQS collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            sqs_client: SqsClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all SQS queues in the configured region
    pub async fn list_queues(&self) -> Result<Vec<SqsQueueUrl>> {
        info!("Listing SQS queues...");
        
        let mut queue_urls = Vec::new();
        let mut paginator = self.sqs_client.list_queues().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(urls) = page.queue_urls {
                for url in urls {
                    queue_urls.push(SqsQueueUrl(url));
                }
            }
        }

        info!("Found {} SQS queues", queue_urls.len());
        Ok(queue_urls)
    }

    /// Extract queue name from queue URL
    /// Queue URLs have format: https://sqs.region.amazonaws.com/account-id/queue-name
    fn extract_queue_name(queue_url: &str) -> String {
        queue_url.split('/').last().unwrap_or(queue_url).to_string()
    }

    /// Collect CloudWatch metrics for a single SQS queue
    pub async fn collect_metrics(
        &self,
        queue_url: &SqsQueueUrl,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<SqsMetrics> {
        let queue_name = Self::extract_queue_name(&queue_url.0);
        
        let mut metrics = SqsMetrics {
            queue_url: queue_url.0.clone(),
            queue_name: queue_name.clone(),
            timestamp: end_time,
            messages_sent: None,
            messages_received: None,
            messages_deleted: None,
            approximate_messages_visible: None,
            approximate_messages_delayed: None,
            approximate_messages_not_visible: None,
            sent_message_size: None,
            number_of_empty_receives: None,
            number_of_messages_returned: None,
            approximate_age_of_oldest_message: None,
        };

        // Collect metrics using helper method
        metrics.messages_sent = self.get_cloudwatch_metric_statistic("NumberOfMessagesSent", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.messages_received = self.get_cloudwatch_metric_statistic("NumberOfMessagesReceived", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.messages_deleted = self.get_cloudwatch_metric_statistic("NumberOfMessagesDeleted", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.approximate_messages_visible = self.get_cloudwatch_metric_statistic("ApproximateNumberOfMessagesVisible", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.approximate_messages_delayed = self.get_cloudwatch_metric_statistic("ApproximateNumberOfMessagesDelayed", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.approximate_messages_not_visible = self.get_cloudwatch_metric_statistic("ApproximateNumberOfMessagesNotVisible", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.sent_message_size = self.get_cloudwatch_metric_statistic("SentMessageSize", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.number_of_empty_receives = self.get_cloudwatch_metric_statistic("NumberOfEmptyReceives", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.number_of_messages_returned = self.get_cloudwatch_metric_statistic("NumberOfMessagesReturned", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.approximate_age_of_oldest_message = self.get_cloudwatch_metric_statistic("ApproximateAgeOfOldestMessage", &queue_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for SQS
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        queue_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("QueueName")
            .value(queue_name)
            .build();

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/SQS")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60) // 1-minute periods for SQS
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
                warn!("Failed to get SQS metric {} for queue {}: {}", metric_name, queue_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple SQS queues in parallel
    pub async fn collect_metrics_batch(
        &self,
        queues: &[SqsQueueUrl],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<SqsMetrics>> {
        let mut tasks = Vec::new();
        for queue_url in queues {
            let collector = self.clone();
            let queue_url_clone = queue_url.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&queue_url_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for SQS queue: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for SqsCollector {
    fn clone(&self) -> Self {
        Self {
            sqs_client: self.sqs_client.clone(),
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

/// Convert SQS metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn sqs_metrics_to_reiver_format(
    metrics: &SqsMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("queue_name:{}", metrics.queue_name),
        format!("queue_url:{}", metrics.queue_url),
        "source:aws_sqs".to_string(),
    ];

    if let Some(value) = metrics.messages_sent {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.messages_sent".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.messages_received {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.messages_received".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.messages_deleted {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.messages_deleted".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.approximate_messages_visible {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.approximate_messages_visible".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.approximate_messages_delayed {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.approximate_messages_delayed".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.approximate_messages_not_visible {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.approximate_messages_not_visible".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.sent_message_size {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.sent_message_size".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.number_of_empty_receives {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.number_of_empty_receives".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.number_of_messages_returned {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.number_of_messages_returned".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.approximate_age_of_oldest_message {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sqs.approximate_age_of_oldest_message".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

