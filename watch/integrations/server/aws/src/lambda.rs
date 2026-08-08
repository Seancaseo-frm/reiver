//! Lambda integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect Lambda function metrics from AWS CloudWatch.
//! Metrics collected include:
//! - Invocations
//! - Duration
//! - Errors
//! - Throttles
//! - ConcurrentExecutions
//! - DeadLetterErrors
//! - DestinationDeliveryFailures

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_lambda::Client as LambdaClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::AwsConfig;

/// Lambda function identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaFunctionName(pub String);

/// Lambda metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct LambdaMetrics {
    pub function_name: String,
    pub timestamp: DateTime<Utc>,
    pub invocations: Option<f64>,
    pub duration: Option<f64>, // Average duration in milliseconds
    pub errors: Option<f64>,
    pub throttles: Option<f64>,
    pub concurrent_executions: Option<f64>,
    pub dead_letter_errors: Option<f64>,
    pub destination_delivery_failures: Option<f64>,
    pub iterator_age: Option<f64>, // For stream processing (milliseconds)
}

/// Lambda metrics collector
pub struct LambdaCollector {
    lambda_client: LambdaClient,
    cloudwatch_client: CloudWatchClient,
}

impl LambdaCollector {
    /// Create a new Lambda collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            lambda_client: LambdaClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all Lambda functions in the configured region
    pub async fn list_functions(&self) -> Result<Vec<LambdaFunctionName>> {
        info!("Listing Lambda functions...");
        let mut function_names = Vec::new();
        let mut paginator = self.lambda_client.list_functions().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(functions) = page.functions {
                for function in functions {
                    if let Some(function_name) = function.function_name {
                        function_names.push(LambdaFunctionName(function_name));
                    }
                }
            }
        }

        info!("Found {} Lambda functions", function_names.len());
        Ok(function_names)
    }

    /// Collect CloudWatch metrics for a single Lambda function
    pub async fn collect_metrics(
        &self,
        function_name: &LambdaFunctionName,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<LambdaMetrics> {
        let mut metrics = LambdaMetrics {
            function_name: function_name.0.clone(),
            timestamp: end_time,
            invocations: None,
            duration: None,
            errors: None,
            throttles: None,
            concurrent_executions: None,
            dead_letter_errors: None,
            destination_delivery_failures: None,
            iterator_age: None,
        };

        // Collect metrics using helper methods
        metrics.invocations = self.get_metric_statistics("Invocations", &function_name.0, start_time, end_time, true).await?;
        metrics.duration = self.get_metric_statistics("Duration", &function_name.0, start_time, end_time, false).await?;
        metrics.errors = self.get_metric_statistics("Errors", &function_name.0, start_time, end_time, true).await?;
        metrics.throttles = self.get_metric_statistics("Throttles", &function_name.0, start_time, end_time, true).await?;
        metrics.concurrent_executions = self.get_metric_statistics("ConcurrentExecutions", &function_name.0, start_time, end_time, true).await?;
        metrics.dead_letter_errors = self.get_metric_statistics("DeadLetterErrors", &function_name.0, start_time, end_time, true).await?;
        metrics.destination_delivery_failures = self.get_metric_statistics("DestinationDeliveryFailures", &function_name.0, start_time, end_time, true).await?;
        metrics.iterator_age = self.get_metric_statistics("IteratorAge", &function_name.0, start_time, end_time, false).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch
    async fn get_metric_statistics(
        &self,
        metric_name: &str,
        function_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        use_sum: bool, // true for Sum statistic, false for Average
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name("FunctionName")
            .value(function_name)
            .build();

        let mut request = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/Lambda")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60); // 1 minute period

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
                tracing::warn!("Failed to get Lambda metric {} for function {}: {}", metric_name, function_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple Lambda functions
    pub async fn collect_metrics_batch(
        &self,
        function_names: &[LambdaFunctionName],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<LambdaMetrics>> {
        let mut metrics = Vec::new();

        for function_name in function_names {
            match self.collect_metrics(function_name, start_time, end_time).await {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for function {}: {}", function_name.0, e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for LambdaCollector {
    fn clone(&self) -> Self {
        Self {
            lambda_client: self.lambda_client.clone(),
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

/// Convert Lambda metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn lambda_metrics_to_reiver_format(
    metrics: &LambdaMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("function_name:{}", metrics.function_name),
        "source:aws_lambda".to_string(),
    ];

    if let Some(invocations) = metrics.invocations {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.invocations".to_string(),
            value: invocations,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(duration) = metrics.duration {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.duration".to_string(),
            value: duration,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(errors) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.errors".to_string(),
            value: errors,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(throttles) = metrics.throttles {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.throttles".to_string(),
            value: throttles,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(concurrent) = metrics.concurrent_executions {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.concurrent_executions".to_string(),
            value: concurrent,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(dead_letter) = metrics.dead_letter_errors {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.dead_letter_errors".to_string(),
            value: dead_letter,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(dest_failures) = metrics.destination_delivery_failures {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.destination_delivery_failures".to_string(),
            value: dest_failures,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(iterator_age) = metrics.iterator_age {
        reiver_metrics.push(ReiverMetric {
            name: "aws.lambda.iterator_age".to_string(),
            value: iterator_age,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

