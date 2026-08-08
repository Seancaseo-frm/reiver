//! DynamoDB integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect DynamoDB table metrics from AWS CloudWatch.
//! Metrics collected include:
//! - ConsumedReadCapacityUnits
//! - ConsumedWriteCapacityUnits
//! - ProvisionedReadCapacityUnits
//! - ProvisionedWriteCapacityUnits
//! - ReadThrottleEvents
//! - WriteThrottleEvents
//! - SystemErrors
//! - UserErrors
//! - SuccessfulRequestLatency
//! - ConditionalCheckFailedRequests
//! - TransactionConflict

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// DynamoDB table identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbTableName(pub String);

/// DynamoDB metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct DynamoDbMetrics {
    pub table_name: String,
    pub timestamp: DateTime<Utc>,
    pub consumed_read_capacity_units: Option<f64>,
    pub consumed_write_capacity_units: Option<f64>,
    pub provisioned_read_capacity_units: Option<f64>,
    pub provisioned_write_capacity_units: Option<f64>,
    pub read_throttle_events: Option<f64>,
    pub write_throttle_events: Option<f64>,
    pub user_errors: Option<f64>,
    pub system_errors: Option<f64>,
    pub conditional_check_failed_requests: Option<f64>,
    pub transaction_conflict: Option<f64>,
    pub successful_request_latency: Option<f64>,
}

/// DynamoDB metrics collector
pub struct DynamoDbCollector {
    dynamodb_client: DynamoDbClient,
    cloudwatch_client: CloudWatchClient,
}

impl DynamoDbCollector {
    /// Create a new DynamoDB collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            dynamodb_client: DynamoDbClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all DynamoDB tables in the configured region
    pub async fn list_tables(&self) -> Result<Vec<DynamoDbTableName>> {
        info!("Listing DynamoDB tables...");
        
        let mut table_names = Vec::new();
        let mut paginator = self.dynamodb_client.list_tables().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(tables) = page.table_names {
                for table_name in tables {
                    table_names.push(DynamoDbTableName(table_name));
                }
            }
        }

        info!("Found {} DynamoDB tables", table_names.len());
        Ok(table_names)
    }

    /// Collect CloudWatch metrics for a single DynamoDB table
    pub async fn collect_metrics(
        &self,
        table_name: &DynamoDbTableName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<DynamoDbMetrics> {
        let mut metrics = DynamoDbMetrics {
            table_name: table_name.0.clone(),
            timestamp: end_time,
            consumed_read_capacity_units: None,
            consumed_write_capacity_units: None,
            provisioned_read_capacity_units: None,
            provisioned_write_capacity_units: None,
            read_throttle_events: None,
            write_throttle_events: None,
            user_errors: None,
            system_errors: None,
            conditional_check_failed_requests: None,
            transaction_conflict: None,
            successful_request_latency: None,
        };

        // Collect metrics using helper methods
        metrics.consumed_read_capacity_units = self.get_metric_statistic("ConsumedReadCapacityUnits", &table_name.0, start_time, end_time, true).await?;
        metrics.consumed_write_capacity_units = self.get_metric_statistic("ConsumedWriteCapacityUnits", &table_name.0, start_time, end_time, true).await?;
        metrics.provisioned_read_capacity_units = self.get_metric_statistic("ProvisionedReadCapacityUnits", &table_name.0, start_time, end_time, false).await?;
        metrics.provisioned_write_capacity_units = self.get_metric_statistic("ProvisionedWriteCapacityUnits", &table_name.0, start_time, end_time, false).await?;
        metrics.read_throttle_events = self.get_metric_statistic("ReadThrottleEvents", &table_name.0, start_time, end_time, true).await?;
        metrics.write_throttle_events = self.get_metric_statistic("WriteThrottleEvents", &table_name.0, start_time, end_time, true).await?;
        metrics.user_errors = self.get_metric_statistic("UserErrors", &table_name.0, start_time, end_time, true).await?;
        metrics.system_errors = self.get_metric_statistic("SystemErrors", &table_name.0, start_time, end_time, true).await?;
        metrics.conditional_check_failed_requests = self.get_metric_statistic("ConditionalCheckFailedRequests", &table_name.0, start_time, end_time, true).await?;
        metrics.transaction_conflict = self.get_metric_statistic("TransactionConflict", &table_name.0, start_time, end_time, true).await?;
        metrics.successful_request_latency = self.get_metric_statistic("SuccessfulRequestLatency", &table_name.0, start_time, end_time, false).await?;

        Ok(metrics)
    }

    /// Get metric statistic from CloudWatch for DynamoDB
    async fn get_metric_statistic(
        &self,
        metric_name: &str,
        table_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("TableName")
            .value(table_name)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/DynamoDB")
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
                warn!("Failed to get DynamoDB metric {} for table {}: {}", metric_name, table_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple DynamoDB tables
    pub async fn collect_metrics_batch(
        &self,
        table_names: &[DynamoDbTableName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DynamoDbMetrics>> {
        let mut metrics = Vec::new();

        for table_name in table_names {
            match self.collect_metrics(table_name, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for DynamoDB table {}: {}", table_name.0, e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for DynamoDbCollector {
    fn clone(&self) -> Self {
        Self {
            dynamodb_client: self.dynamodb_client.clone(),
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

/// Convert DynamoDB metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn dynamodb_metrics_to_reiver_format(
    metrics: &DynamoDbMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("table_name:{}", metrics.table_name),
        "source:aws_dynamodb".to_string(),
    ];

    if let Some(consumed_read) = metrics.consumed_read_capacity_units {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.consumed_read_capacity_units".to_string(),
            value: consumed_read,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(consumed_write) = metrics.consumed_write_capacity_units {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.consumed_write_capacity_units".to_string(),
            value: consumed_write,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(provisioned_read) = metrics.provisioned_read_capacity_units {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.provisioned_read_capacity_units".to_string(),
            value: provisioned_read,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(provisioned_write) = metrics.provisioned_write_capacity_units {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.provisioned_write_capacity_units".to_string(),
            value: provisioned_write,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(read_throttles) = metrics.read_throttle_events {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.read_throttle_events".to_string(),
            value: read_throttles,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(write_throttles) = metrics.write_throttle_events {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.write_throttle_events".to_string(),
            value: write_throttles,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(user_errors) = metrics.user_errors {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.user_errors".to_string(),
            value: user_errors,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(system_errors) = metrics.system_errors {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.system_errors".to_string(),
            value: system_errors,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(conditional_failed) = metrics.conditional_check_failed_requests {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.conditional_check_failed_requests".to_string(),
            value: conditional_failed,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(conflict) = metrics.transaction_conflict {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.transaction_conflict".to_string(),
            value: conflict,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(latency) = metrics.successful_request_latency {
        reiver_metrics.push(ReiverMetric {
            name: "aws.dynamodb.successful_request_latency".to_string(),
            value: latency,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
