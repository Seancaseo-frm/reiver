//! API Gateway integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect API Gateway REST API metrics from AWS CloudWatch.
//! Metrics collected include:
//! - Count (total number of requests)
//! - Latency (request latency)
//! - 4XXError (client errors)
//! - 5XXError (server errors)
//! - IntegrationLatency (backend latency)
//! - CacheHitCount
//! - CacheMissCount

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_apigateway::Client as ApiGatewayClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// API Gateway REST API identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayRestApiId(pub String);

/// API Gateway stage identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayStage {
    pub rest_api_id: String,
    pub stage_name: String,
}

/// API Gateway metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct ApiGatewayMetrics {
    pub rest_api_id: String,
    pub stage_name: String,
    pub timestamp: DateTime<Utc>,
    pub count: Option<f64>,
    pub latency: Option<f64>,
    pub errors_4xx: Option<f64>,
    pub errors_5xx: Option<f64>,
    pub integration_latency: Option<f64>,
    pub cache_hit_count: Option<f64>,
    pub cache_miss_count: Option<f64>,
}

/// API Gateway metrics collector
pub struct ApiGatewayCollector {
    apigateway_client: ApiGatewayClient,
    cloudwatch_client: CloudWatchClient,
}

impl ApiGatewayCollector {
    /// Create a new API Gateway collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            apigateway_client: ApiGatewayClient::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all REST APIs in the configured region
    pub async fn list_rest_apis(&self) -> Result<Vec<ApiGatewayRestApiId>> {
        info!("Listing API Gateway REST APIs...");
        
        let mut api_ids = Vec::new();
        let mut paginator = self.apigateway_client.get_rest_apis().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(items) = page.items {
                for item in items {
                    if let Some(api_id) = item.id {
                        api_ids.push(ApiGatewayRestApiId(api_id));
                    }
                }
            }
        }

        info!("Found {} API Gateway REST APIs", api_ids.len());
        Ok(api_ids)
    }

    /// List all stages for a REST API
    pub async fn list_stages(&self, rest_api_id: &str) -> Result<Vec<String>> {
        let response = self.apigateway_client
            .get_stages()
            .rest_api_id(rest_api_id)
            .send()
            .await?;

        let mut stage_names = Vec::new();
        if let Some(items) = response.item {
            for stage in items {
                if let Some(stage_name) = stage.stage_name {
                    stage_names.push(stage_name);
                }
            }
        }

        Ok(stage_names)
    }

    /// Collect CloudWatch metrics for a single API Gateway stage
    pub async fn collect_metrics(
        &self,
        stage: &ApiGatewayStage,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<ApiGatewayMetrics> {
        let mut metrics = ApiGatewayMetrics {
            rest_api_id: stage.rest_api_id.clone(),
            stage_name: stage.stage_name.clone(),
            timestamp: end_time,
            count: None,
            latency: None,
            errors_4xx: None,
            errors_5xx: None,
            integration_latency: None,
            cache_hit_count: None,
            cache_miss_count: None,
        };

        // Collect metrics using helper method
        // API Gateway metrics use dimensions: ApiName and Stage
        metrics.count = self.get_cloudwatch_metric_statistic("Count", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.latency = self.get_cloudwatch_metric_statistic("Latency", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.errors_4xx = self.get_cloudwatch_metric_statistic("4XXError", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.errors_5xx = self.get_cloudwatch_metric_statistic("5XXError", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.integration_latency = self.get_cloudwatch_metric_statistic("IntegrationLatency", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Average).await?;
        metrics.cache_hit_count = self.get_cloudwatch_metric_statistic("CacheHitCount", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;
        metrics.cache_miss_count = self.get_cloudwatch_metric_statistic("CacheMissCount", &stage.rest_api_id, &stage.stage_name, start_time, end_time, aws_sdk_cloudwatch::types::Statistic::Sum).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for API Gateway
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        rest_api_id: &str,
        stage_name: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        // API Gateway metrics use two dimensions: ApiName and Stage
        let dimensions = vec![
            aws_sdk_cloudwatch::types::Dimension::builder()
                .name("ApiName")
                .value(rest_api_id)
                .build(),
            aws_sdk_cloudwatch::types::Dimension::builder()
                .name("Stage")
                .value(stage_name)
                .build(),
        ];

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/ApiGateway")
            .metric_name(metric_name)
            .set_dimensions(Some(dimensions))
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60) // 1-minute periods for API Gateway
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
                warn!("Failed to get API Gateway metric {} for API {} stage {}: {}", metric_name, rest_api_id, stage_name, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple API Gateway stages in parallel
    pub async fn collect_metrics_batch(
        &self,
        stages: &[ApiGatewayStage],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<ApiGatewayMetrics>> {
        let mut tasks = Vec::new();
        for stage in stages {
            let collector = self.clone();
            let stage_clone = stage.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_metrics(&stage_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for API Gateway stage: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for ApiGatewayCollector {
    fn clone(&self) -> Self {
        Self {
            apigateway_client: self.apigateway_client.clone(),
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

/// Convert API Gateway metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn apigateway_metrics_to_reiver_format(
    metrics: &ApiGatewayMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("rest_api_id:{}", metrics.rest_api_id),
        format!("stage_name:{}", metrics.stage_name),
        "source:aws_apigateway".to_string(),
    ];

    if let Some(value) = metrics.count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.latency {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors_4xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.errors_4xx".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors_5xx {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.errors_5xx".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.integration_latency {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.integration_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_hit_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.cache_hit_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.cache_miss_count {
        reiver_metrics.push(ReiverMetric {
            name: "aws.apigateway.cache_miss_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

