//! Google Cloud API Gateway integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud API Gateway metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Request Count
//! - Request Latency
//! - Backend Latency
//! - Errors
//! - Request/Response Bytes

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::GcpConfig;

/// Google Cloud API Gateway identifier (gateway name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayId(pub String);

/// Google Cloud API Gateway metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct ApiGatewayMetrics {
    pub gateway_id: String,
    pub gateway_name: String,
    pub api_name: String,
    pub region: String,
    pub timestamp: DateTime<Utc>,
    pub request_count: Option<f64>,
    pub request_latency: Option<f64>,
    pub backend_latency: Option<f64>,
    pub request_bytes: Option<f64>,
    pub response_bytes: Option<f64>,
    pub errors: Option<f64>,
    pub error_rate: Option<f64>,
}

/// Cloud Monitoring API response structures (reused from compute module)
#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct TimeSeriesResponse {
    timeSeries: Vec<TimeSeries>,
    #[serde(default)]
    nextPageToken: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct TimeSeries {
    metric: Metric,
    resource: Resource,
    points: Vec<Point>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Metric {
    #[serde(rename = "type")]
    metric_type: String,
    labels: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Resource {
    #[serde(rename = "type")]
    resource_type: String,
    labels: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Point {
    interval: Interval,
    value: Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Interval {
    endTime: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct Value {
    doubleValue: Option<f64>,
    int64Value: Option<String>,
}

/// Google Cloud API Gateway metrics collector
pub struct ApiGatewayCollector {
    config: GcpConfig,
    http_client: Client,
}

impl ApiGatewayCollector {
    /// Create a new API Gateway collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// Collect Cloud Monitoring metrics for API Gateways
    /// Note: We discover API Gateways by querying Cloud Monitoring metrics directly
    pub async fn collect_all_metrics(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<ApiGatewayMetrics>> {
        info!("Collecting Google Cloud API Gateway metrics...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for API Gateway metrics
        let filter = "resource.type = \"apigateway.googleapis.com/Gateway\"";
        
        // Metrics to collect
        let metrics = vec![
            "apigateway.googleapis.com/api/request_count",
            "apigateway.googleapis.com/api/request_latencies",
            "apigateway.googleapis.com/api/backend_latencies",
            "apigateway.googleapis.com/api/request_bytes",
            "apigateway.googleapis.com/api/response_bytes",
        ];
        
        let metrics_str = metrics.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
        let start_time_rfc3339 = start_time.to_rfc3339();
        let end_time_rfc3339 = end_time.to_rfc3339();
        
        let request_body = serde_json::json!({
            "filter": full_filter,
            "interval": {
                "startTime": start_time_rfc3339,
                "endTime": end_time_rfc3339,
            },
            "aggregation": {
                "alignmentPeriod": "60s",
                "perSeriesAligner": "ALIGN_RATE",
                "crossSeriesReducer": "REDUCE_SUM",
                "groupByFields": ["resource.labels.gateway_id", "resource.labels.api_id", "resource.labels.location"],
            },
        });
        
        let response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Cloud Monitoring metrics: {}", e))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("Cloud Monitoring API error ({}): {}", status, body);
            return Ok(Vec::new()); // Return empty on error, don't fail completely
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
        
        // Group metrics by gateway (gateway_id + api_id + location)
        let mut gateway_metrics: std::collections::HashMap<(String, String, String), ApiGatewayMetrics> = std::collections::HashMap::new();
        
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
            // Extract gateway info from resource labels
            let (gateway_id, api_id, region) = if let Some(labels) = &time_series.resource.labels {
                let gw_id = labels.get("gateway_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let api = labels.get("api_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let loc = labels.get("location")
                    .or_else(|| labels.get("region"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("global")
                    .to_string();
                (gw_id, api, loc)
            } else {
                ("unknown".to_string(), "unknown".to_string(), "global".to_string())
            };
            
            let key = (gateway_id.clone(), api_id.clone(), region.clone());
            
            // Get or create metrics for this gateway
            let metrics = gateway_metrics.entry(key).or_insert_with(|| ApiGatewayMetrics {
                gateway_id: gateway_id.clone(),
                gateway_name: gateway_id.clone(),
                api_name: api_id.clone(),
                region: region.clone(),
                timestamp: end_time,
                request_count: None,
                request_latency: None,
                backend_latency: None,
                request_bytes: None,
                response_bytes: None,
                errors: None,
                error_rate: None,
            });
            
            // Get the latest data point
            let latest_value = time_series.points
                .iter()
                .filter_map(|point| {
                    point.value.doubleValue
                        .or_else(|| {
                            point.value.int64Value
                                .as_ref()
                                .and_then(|v| v.parse::<f64>().ok())
                        })
                })
                .last();
            
            // Map metric types to fields
            match metric_type.as_str() {
                "apigateway.googleapis.com/api/request_count" => {
                    metrics.request_count = latest_value;
                }
                "apigateway.googleapis.com/api/request_latencies" => {
                    metrics.request_latency = latest_value;
                }
                "apigateway.googleapis.com/api/backend_latencies" => {
                    metrics.backend_latency = latest_value;
                }
                "apigateway.googleapis.com/api/request_bytes" => {
                    metrics.request_bytes = latest_value;
                }
                "apigateway.googleapis.com/api/response_bytes" => {
                    metrics.response_bytes = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        // Separate request for errors
        let errors_filter = format!(
            "{} AND metric.type = \"apigateway.googleapis.com/api/request_count\" AND metric.labels.response_code_class = \"5xx\"",
            filter
        );
        
        let errors_request_body = serde_json::json!({
            "filter": errors_filter,
            "interval": {
                "startTime": start_time_rfc3339,
                "endTime": end_time_rfc3339,
            },
            "aggregation": {
                "alignmentPeriod": "60s",
                "perSeriesAligner": "ALIGN_RATE",
                "crossSeriesReducer": "REDUCE_SUM",
                "groupByFields": ["resource.labels.gateway_id", "resource.labels.api_id", "resource.labels.location"],
            },
        });
        
        let errors_response = self.http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .json(&errors_request_body)
            .send()
            .await;
        
        // Parse errors if available
        if let Ok(response) = errors_response {
            if response.status().is_success() {
                if let Ok(errors_metrics) = response.json::<TimeSeriesResponse>().await {
                    for time_series in errors_metrics.timeSeries {
                        let (gateway_id, api_id, region) = if let Some(labels) = &time_series.resource.labels {
                            let gw_id = labels.get("gateway_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let api = labels.get("api_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let loc = labels.get("location")
                                .or_else(|| labels.get("region"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("global")
                                .to_string();
                            (gw_id, api, loc)
                        } else {
                            ("unknown".to_string(), "unknown".to_string(), "global".to_string())
                        };
                        
                        let key = (gateway_id, api_id, region);
                        
                        if let Some(metrics) = gateway_metrics.get_mut(&key) {
                            let latest_value = time_series.points
                                .iter()
                                .filter_map(|point| {
                                    point.value.doubleValue
                                        .or_else(|| {
                                            point.value.int64Value
                                                .as_ref()
                                                .and_then(|v| v.parse::<f64>().ok())
                                        })
                                })
                                .last();
                            
                            if let Some(value) = latest_value {
                                metrics.errors = Some(value);
                                
                                // Calculate error rate if we have request count
                                if let Some(req_count) = metrics.request_count {
                                    if req_count > 0.0 {
                                        metrics.error_rate = Some(value / req_count * 100.0);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(gateway_metrics.into_values().collect())
    }
}

impl Clone for ApiGatewayCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            http_client: Client::new(),
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
pub fn api_gateway_metrics_to_reiver_format(
    metrics: &ApiGatewayMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("gateway_id:{}", metrics.gateway_id),
        format!("gateway_name:{}", metrics.gateway_name),
        format!("api_name:{}", metrics.api_name),
        format!("region:{}", metrics.region),
        "source:gcp_api_gateway".to_string(),
    ];

    if let Some(value) = metrics.request_count {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.request_count".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_latency {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.request_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.backend_latency {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.backend_latency".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.request_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.request_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.response_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.response_bytes".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.errors".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.error_rate {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.api_gateway.error_rate".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
