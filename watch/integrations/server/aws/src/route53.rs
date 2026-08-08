//! Route53 integration for collecting CloudWatch metrics
//!
//! This module provides functionality to collect Route53 DNS metrics from AWS CloudWatch.
//! Metrics collected include:
//! - DNSQueries (total DNS queries per hosted zone)
//! - HealthCheckStatus (health check status - 0 or 1)
//! - HealthCheckPercentageHealthy (percentage of health checks that are healthy)
//! - ConnectionTime (health check connection time)
//! - SSLHandshakeTime (health check SSL handshake time)
//! - TimeToFirstByte (health check time to first byte)

use anyhow::Result;
use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_route53::Client as Route53Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::AwsConfig;

/// Route53 hosted zone identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route53HostedZoneId(pub String);

/// Route53 health check identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route53HealthCheckId(pub String);

/// Route53 hosted zone metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct Route53HostedZoneMetrics {
    pub hosted_zone_id: String,
    pub timestamp: DateTime<Utc>,
    pub dns_queries: Option<f64>,
}

/// Route53 health check metrics collected from CloudWatch
#[derive(Debug, Clone, Serialize)]
pub struct Route53HealthCheckMetrics {
    pub health_check_id: String,
    pub timestamp: DateTime<Utc>,
    pub health_check_status: Option<f64>,
    pub health_check_percentage_healthy: Option<f64>,
    pub connection_time: Option<f64>,
    pub ssl_handshake_time: Option<f64>,
    pub time_to_first_byte: Option<f64>,
}

/// Route53 metrics collector
pub struct Route53Collector {
    route53_client: Route53Client,
    cloudwatch_client: CloudWatchClient,
}

impl Route53Collector {
    /// Create a new Route53 collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and default credential chain
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            route53_client: Route53Client::new(&aws_config),
            cloudwatch_client: CloudWatchClient::new(&aws_config),
        })
    }

    /// List all hosted zones in the configured region
    pub async fn list_hosted_zones(&self) -> Result<Vec<Route53HostedZoneId>> {
        info!("Listing Route53 hosted zones...");
        
        let mut hosted_zone_ids = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut request = self.route53_client.list_hosted_zones();
            if let Some(ref marker_value) = marker {
                request = request.marker(marker_value);
            }

            let response = request.send().await?;
            
            // Check if there are more hosted zones to fetch before consuming the response
            let is_truncated = response.is_truncated();
            let next_marker = response.next_marker().map(|s| s.to_string());
            
            for zone in response.hosted_zones {
                // Extract just the ID part (remove /hostedzone/ prefix)
                let zone_id_clean = zone.id
                    .trim_start_matches("/hostedzone/")
                    .to_string();
                hosted_zone_ids.push(Route53HostedZoneId(zone_id_clean));
            }

            // Check if there are more hosted zones to fetch
            if is_truncated {
                marker = next_marker;
            } else {
                break;
            }
        }

        info!("Found {} Route53 hosted zones", hosted_zone_ids.len());
        Ok(hosted_zone_ids)
    }

    /// List all health checks
    pub async fn list_health_checks(&self) -> Result<Vec<Route53HealthCheckId>> {
        info!("Listing Route53 health checks...");
        
        let mut health_check_ids = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut request = self.route53_client.list_health_checks();
            if let Some(ref marker_value) = marker {
                request = request.marker(marker_value);
            }

            let response = request.send().await?;
            
            // Check if there are more health checks to fetch before consuming the response
            let is_truncated = response.is_truncated();
            let next_marker = response.next_marker().map(|s| s.to_string());
            
            for health_check in response.health_checks {
                health_check_ids.push(Route53HealthCheckId(health_check.id));
            }

            // Check if there are more health checks to fetch
            if is_truncated {
                marker = next_marker;
            } else {
                break;
            }
        }

        info!("Found {} Route53 health checks", health_check_ids.len());
        Ok(health_check_ids)
    }

    /// Collect CloudWatch metrics for a single Route53 hosted zone
    pub async fn collect_hosted_zone_metrics(
        &self,
        hosted_zone_id: &Route53HostedZoneId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Route53HostedZoneMetrics> {
        let mut metrics = Route53HostedZoneMetrics {
            hosted_zone_id: hosted_zone_id.0.clone(),
            timestamp: end_time,
            dns_queries: None,
        };

        // Collect DNSQueries metric for the hosted zone
        metrics.dns_queries = self.get_cloudwatch_metric_statistic(
            "DNSQueries",
            "HostedZoneId",
            &hosted_zone_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Sum,
        ).await?;

        Ok(metrics)
    }

    /// Collect CloudWatch metrics for a single Route53 health check
    pub async fn collect_health_check_metrics(
        &self,
        health_check_id: &Route53HealthCheckId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Route53HealthCheckMetrics> {
        let mut metrics = Route53HealthCheckMetrics {
            health_check_id: health_check_id.0.clone(),
            timestamp: end_time,
            health_check_status: None,
            health_check_percentage_healthy: None,
            connection_time: None,
            ssl_handshake_time: None,
            time_to_first_byte: None,
        };

        // Collect health check metrics
        metrics.health_check_status = self.get_cloudwatch_metric_statistic(
            "HealthCheckStatus",
            "HealthCheckId",
            &health_check_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Average,
        ).await?;

        metrics.health_check_percentage_healthy = self.get_cloudwatch_metric_statistic(
            "HealthCheckPercentageHealthy",
            "HealthCheckId",
            &health_check_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Average,
        ).await?;

        metrics.connection_time = self.get_cloudwatch_metric_statistic(
            "ConnectionTime",
            "HealthCheckId",
            &health_check_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Average,
        ).await?;

        metrics.ssl_handshake_time = self.get_cloudwatch_metric_statistic(
            "SSLHandshakeTime",
            "HealthCheckId",
            &health_check_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Average,
        ).await?;

        metrics.time_to_first_byte = self.get_cloudwatch_metric_statistic(
            "TimeToFirstByte",
            "HealthCheckId",
            &health_check_id.0,
            start_time,
            end_time,
            aws_sdk_cloudwatch::types::Statistic::Average,
        ).await?;

        Ok(metrics)
    }

    /// Get metric statistics from CloudWatch for Route53
    async fn get_cloudwatch_metric_statistic(
        &self,
        metric_name: &str,
        dimension_name: &str,
        dimension_value: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        statistic: aws_sdk_cloudwatch::types::Statistic,
    ) -> Result<Option<f64>> {
        use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;

        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let dimension = aws_sdk_cloudwatch::types::Dimension::builder()
            .name(dimension_name)
            .value(dimension_value)
            .build();

        let response = self.cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/Route53")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time(start_aws)
            .end_time(end_aws)
            .period(60) // 1-minute periods for Route53
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
                warn!("Failed to get Route53 metric {} for {} {}: {}", metric_name, dimension_name, dimension_value, e);
                Ok(None) // Return None instead of error to allow partial metrics
            }
        }
    }

    /// Collect CloudWatch metrics for multiple hosted zones in parallel
    pub async fn collect_hosted_zone_metrics_batch(
        &self,
        hosted_zones: &[Route53HostedZoneId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<Route53HostedZoneMetrics>> {
        let mut tasks = Vec::new();
        for hosted_zone_id in hosted_zones {
            let collector = self.clone();
            let hosted_zone_id_clone = hosted_zone_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_hosted_zone_metrics(&hosted_zone_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Route53 hosted zone: {}", e);
                }
            }
        }

        Ok(metrics)
    }

    /// Collect CloudWatch metrics for multiple health checks in parallel
    pub async fn collect_health_check_metrics_batch(
        &self,
        health_checks: &[Route53HealthCheckId],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<Route53HealthCheckMetrics>> {
        let mut tasks = Vec::new();
        for health_check_id in health_checks {
            let collector = self.clone();
            let health_check_id_clone = health_check_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_health_check_metrics(&health_check_id_clone, start_time, end_time).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect metrics for Route53 health check: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for Route53Collector {
    fn clone(&self) -> Self {
        Self {
            route53_client: self.route53_client.clone(),
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

/// Convert Route53 hosted zone metrics to Reiver metric format
pub fn route53_hosted_zone_metrics_to_reiver_format(
    metrics: &Route53HostedZoneMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("hosted_zone_id:{}", metrics.hosted_zone_id),
        "source:aws_route53".to_string(),
    ];

    if let Some(value) = metrics.dns_queries {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.dns_queries".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}

/// Convert Route53 health check metrics to Reiver metric format
pub fn route53_health_check_metrics_to_reiver_format(
    metrics: &Route53HealthCheckMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("health_check_id:{}", metrics.health_check_id),
        "source:aws_route53".to_string(),
    ];

    if let Some(value) = metrics.health_check_status {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.health_check_status".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.health_check_percentage_healthy {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.health_check_percentage_healthy".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.connection_time {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.connection_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.ssl_handshake_time {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.ssl_handshake_time".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.time_to_first_byte {
        reiver_metrics.push(ReiverMetric {
            name: "aws.route53.time_to_first_byte".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
