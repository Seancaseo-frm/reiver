//! SNS integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect SNS topic metrics from AWS CloudWatch.
//! Metrics collected include:
//! - NumberOfMessagesPublished
//! - NumberOfNotificationsDelivered
//! - NumberOfNotificationsFailed
//! - PublishSize
//! - NumberOfNotificationsFilteredOut
//! - NumberOfNotificationsFilteredOut-NoMessageAttributes
//! - NumberOfNotificationsFilteredOut-InvalidAttributes

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_sns::Client as SnsClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// SNS topic identifier (topic ARN)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnsTopicArn(pub String);

/// SNS topic metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct SnsMetrics {
    pub topic_arn: String,
    pub topic_name: String,
    pub timestamp: DateTime<Utc>,
    pub messages_published: Option<f64>,
    pub notifications_delivered: Option<f64>,
    pub notifications_failed: Option<f64>,
    pub publish_size: Option<f64>,
    pub notifications_filtered_out: Option<f64>,
    pub notifications_filtered_out_no_attributes: Option<f64>,
    pub notifications_filtered_out_invalid_attributes: Option<f64>,
}

/// SNS metrics collector
pub struct SnsCollector {
    sns_client: SnsClient,
    cloudwatch_client: CloudWatchClient,
}

impl SnsCollector {
    /// Create a new SNS collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            sns_client: SnsClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all SNS topics in the configured region
    pub async fn list_topics(&self) -> Result<Vec<SnsTopicArn>> {
        info!("Listing SNS topics...");
        
        let mut topic_arns = Vec::new();
        let mut paginator = self.sns_client.list_topics().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(topics) = page.topics {
                for topic in topics {
                    if let Some(arn) = topic.topic_arn {
                        topic_arns.push(SnsTopicArn(arn));
                    }
                }
            }
        }

        info!("Found {} SNS topics", topic_arns.len());
        Ok(topic_arns)
    }

    /// Extract topic name from topic ARN
    /// Topic ARNs have format: arn:aws:sns:region:account-id:topic-name
    fn extract_topic_name(topic_arn: &str) -> String {
        topic_arn.split(':').last().unwrap_or(topic_arn).to_string()
    }

    /// Collect CloudWatch metrics for a single SNS topic
    pub async fn collect_metrics(
        &self,
        topic_arn: &SnsTopicArn,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<SnsMetrics> {
        let topic_name = Self::extract_topic_name(&topic_arn.0);
        
        let mut metrics = SnsMetrics {
            topic_arn: topic_arn.0.clone(),
            topic_name: topic_name.clone(),
            timestamp: end_time,
            messages_published: None,
            notifications_delivered: None,
            notifications_failed: None,
            publish_size: None,
            notifications_filtered_out: None,
            notifications_filtered_out_no_attributes: None,
            notifications_filtered_out_invalid_attributes: None,
        };

        // Collect metrics using helper method
        metrics.messages_published = self.get_cloudwatch_metric_statistic("NumberOfMessagesPublished", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.notifications_delivered = self.get_cloudwatch_metric_statistic("NumberOfNotificationsDelivered", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.notifications_failed = self.get_cloudwatch_metric_statistic("NumberOfNotificationsFailed", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.publish_size = self.get_cloudwatch_metric_statistic("PublishSize", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.notifications_filtered_out = self.get_cloudwatch_metric_statistic("NumberOfNotificationsFilteredOut", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.notifications_filtered_out_no_attributes = self.get_cloudwatch_metric_statistic("NumberOfNotificationsFilteredOut-NoMessageAttributes", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.notifications_filtered_out_invalid_attributes = self.get_cloudwatch_metric_statistic("NumberOfNotificationsFilteredOut-InvalidAttributes", &topic_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for SNS
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        topic_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("TopicName")
            .value(topic_name)
            .build();

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/SNS")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60) // 1-minute periods for SNS
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
                warn!("Failed to get SNS metric {} for topic {}: {}", metric_name, topic_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple SNS topics in parallel
    pub async fn collect_metrics_batch(
        &self,
        topics: &[SnsTopicArn],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<SnsMetrics>> {
        let mut tasks = Vec::new();
        for topic_arn in topics {
            let collector = self.clone();
            let topic_arn_clone = topic_arn.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&topic_arn_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for SNS topic: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for SnsCollector {
    fn clone(&self) -> Self {
        Self {
            sns_client: self.sns_client.clone(),
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

/// Convert SNS metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn sns_metrics_to_reiver_format(
    metrics: &SnsMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("topic_name:{}", metrics.topic_name),
        format!("topic_arn:{}", metrics.topic_arn),
        "source:aws_sns".to_string(),
    ];

    if let Some(value) = metrics.messages_published {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.messages_published".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.notifications_delivered {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.notifications_delivered".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.notifications_failed {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.notifications_failed".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.publish_size {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.publish_size".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.notifications_filtered_out {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.notifications_filtered_out".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.notifications_filtered_out_no_attributes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.notifications_filtered_out_no_attributes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.notifications_filtered_out_invalid_attributes {
        reiver_metrics.push(ReiverMetric {
            name: "aws.sns.notifications_filtered_out_invalid_attributes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

