//! Google Cloud Pub/Sub integration for collecting Cloud Monitoring metrics
//!
//! This module provides functionality to collect Google Cloud Pub/Sub metrics from Cloud Monitoring API.
//! Metrics collected include:
//! - Message Count (published, delivered)
//! - Message Sizes
//! - Subscription Backlog (undelivered messages)
//! - Oldest Unacked Message Age
//! - Throughput
//! - Errors

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::GcpConfig;

/// Google Cloud Pub/Sub topic identifier (topic name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubTopicId(pub String);

/// Google Cloud Pub/Sub subscription identifier (subscription name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubSubscriptionId(pub String);

/// Google Cloud Pub/Sub metrics collected from Cloud Monitoring
#[derive(Debug, Clone, Serialize)]
pub struct PubSubMetrics {
    pub resource_id: String,
    pub resource_name: String,
    pub resource_type: String, // "topic" or "subscription"
    pub timestamp: DateTime<Utc>,
    pub message_count: Option<f64>, // Published (for topics) or delivered (for subscriptions)
    pub message_bytes: Option<f64>, // Message sizes
    pub undelivered_messages: Option<f64>, // Subscription backlog
    pub oldest_unacked_message_age: Option<f64>, // Age in seconds
    pub send_operations: Option<f64>, // Send request count
    pub pull_operations: Option<f64>, // Pull request count
    pub errors: Option<f64>, // Error count
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

/// Google Cloud Pub/Sub metrics collector
pub struct PubSubCollector {
    config: GcpConfig,
    http_client: Client,
}

impl PubSubCollector {
    /// Create a new Pub/Sub collector with the given GCP configuration
    pub fn new(config: GcpConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    /// List all Pub/Sub topics in the project
    pub async fn list_topics(&self) -> Result<Vec<PubSubTopicId>> {
        info!("Listing Google Cloud Pub/Sub topics...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all topics in the project
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics",
            self.config.project_id
        );
        
        let mut all_topics = Vec::new();
        let mut next_page_token: Option<String> = None;
        
        loop {
            let mut request_url = url.clone();
            if let Some(token) = &next_page_token {
                request_url = format!("{}?pageToken={}", url, token);
            }
            
            let response = self.http_client
                .get(&request_url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Pub/Sub topics: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let topics_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract topic names
            if let Some(topics) = topics_response.get("topics").and_then(|v| v.as_array()) {
                for topic in topics {
                    if let Some(name) = topic.get("name").and_then(|v| v.as_str()) {
                        // Extract topic name from full resource name
                        // Format: projects/{project}/topics/{topic_name}
                        let topic_name = name.split('/').last().unwrap_or(name).to_string();
                        all_topics.push(PubSubTopicId(topic_name));
                    }
                }
            }
            
            // Check for next page
            next_page_token = topics_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Cloud Pub/Sub topics", all_topics.len());
        Ok(all_topics)
    }

    /// List all Pub/Sub subscriptions in the project
    pub async fn list_subscriptions(&self) -> Result<Vec<PubSubSubscriptionId>> {
        info!("Listing Google Cloud Pub/Sub subscriptions...");
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // List all subscriptions in the project
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/subscriptions",
            self.config.project_id
        );
        
        let mut all_subscriptions = Vec::new();
        let mut next_page_token: Option<String> = None;
        
        loop {
            let mut request_url = url.clone();
            if let Some(token) = &next_page_token {
                request_url = format!("{}?pageToken={}", url, token);
            }
            
            let response = self.http_client
                .get(&request_url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to list Pub/Sub subscriptions: {}", e))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("GCP API error ({}): {}", status, body));
            }
            
            let subscriptions_response: serde_json::Value = response.json().await
                .map_err(|e| anyhow::anyhow!("Failed to parse GCP API response: {}", e))?;
            
            // Extract subscription names
            if let Some(subscriptions) = subscriptions_response.get("subscriptions").and_then(|v| v.as_array()) {
                for subscription in subscriptions {
                    if let Some(name) = subscription.get("name").and_then(|v| v.as_str()) {
                        // Extract subscription name from full resource name
                        // Format: projects/{project}/subscriptions/{subscription_name}
                        let subscription_name = name.split('/').last().unwrap_or(name).to_string();
                        all_subscriptions.push(PubSubSubscriptionId(subscription_name));
                    }
                }
            }
            
            // Check for next page
            next_page_token = subscriptions_response.get("nextPageToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            if next_page_token.is_none() {
                break;
            }
        }
        
        info!("Found {} Google Cloud Pub/Sub subscriptions", all_subscriptions.len());
        Ok(all_subscriptions)
    }

    /// Collect Cloud Monitoring metrics for a specific Pub/Sub topic
    pub async fn collect_topic_metrics(
        &self,
        topic_id: &PubSubTopicId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<PubSubMetrics> {
        info!("Collecting Pub/Sub topic metrics for: {}", topic_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific topic
        let filter = format!(
            "resource.type = \"pubsub_topic\" AND resource.labels.topic_id = \"{}\"",
            topic_id.0
        );
        
        // Metrics to collect for topics
        let metrics = vec![
            "pubsub.googleapis.com/topic/send_request_count",
            "pubsub.googleapis.com/topic/message_sizes",
            "pubsub.googleapis.com/topic/send_operation_count",
        ];
        
        // Build filter with metric types
        let metrics_str = metrics.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
        // Format times for Cloud Monitoring API (RFC3339)
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
            return Err(anyhow::anyhow!("Cloud Monitoring API error ({}): {}", status, body));
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
        
        let mut topic_metrics = PubSubMetrics {
            resource_id: topic_id.0.clone(),
            resource_name: topic_id.0.clone(),
            resource_type: "topic".to_string(),
            timestamp: end_time,
            message_count: None,
            message_bytes: None,
            undelivered_messages: None,
            oldest_unacked_message_age: None,
            send_operations: None,
            pull_operations: None,
            errors: None,
        };
        
        // Parse metrics
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
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
            
            match metric_type.as_str() {
                "pubsub.googleapis.com/topic/send_request_count" => {
                    topic_metrics.message_count = latest_value;
                }
                "pubsub.googleapis.com/topic/message_sizes" => {
                    topic_metrics.message_bytes = latest_value;
                }
                "pubsub.googleapis.com/topic/send_operation_count" => {
                    topic_metrics.send_operations = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        Ok(topic_metrics)
    }

    /// Collect Cloud Monitoring metrics for a specific Pub/Sub subscription
    pub async fn collect_subscription_metrics(
        &self,
        subscription_id: &PubSubSubscriptionId,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<PubSubMetrics> {
        info!("Collecting Pub/Sub subscription metrics for: {}", subscription_id.0);
        
        let access_token = self.config
            .get_access_token()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get GCP access token: {}", e))?;
        
        // Build Cloud Monitoring API URL
        let url = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        );
        
        // Build filter for this specific subscription
        let filter = format!(
            "resource.type = \"pubsub_subscription\" AND resource.labels.subscription_id = \"{}\"",
            subscription_id.0
        );
        
        // Metrics to collect for subscriptions
        let metrics = vec![
            "pubsub.googleapis.com/subscription/send_request_count",
            "pubsub.googleapis.com/subscription/num_undelivered_messages",
            "pubsub.googleapis.com/subscription/oldest_unacked_message_age",
            "pubsub.googleapis.com/subscription/pull_request_count",
        ];
        
        // Build filter with metric types
        let metrics_str = metrics.iter().map(|m| format!("metric.type = \"{}\"", m)).collect::<Vec<_>>().join(" OR ");
        let full_filter = format!("{} AND ({})", filter, metrics_str);
        
        // Format times for Cloud Monitoring API (RFC3339)
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
                "perSeriesAligner": "ALIGN_MEAN",
                "crossSeriesReducer": "REDUCE_MEAN",
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
            return Err(anyhow::anyhow!("Cloud Monitoring API error ({}): {}", status, body));
        }
        
        let metrics_response: TimeSeriesResponse = response.json().await
            .map_err(|e| anyhow::anyhow!("Failed to parse Cloud Monitoring metrics response: {}", e))?;
        
        let mut subscription_metrics = PubSubMetrics {
            resource_id: subscription_id.0.clone(),
            resource_name: subscription_id.0.clone(),
            resource_type: "subscription".to_string(),
            timestamp: end_time,
            message_count: None,
            message_bytes: None,
            undelivered_messages: None,
            oldest_unacked_message_age: None,
            send_operations: None,
            pull_operations: None,
            errors: None,
        };
        
        // Parse metrics
        for time_series in metrics_response.timeSeries {
            let metric_type = &time_series.metric.metric_type;
            
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
            
            match metric_type.as_str() {
                "pubsub.googleapis.com/subscription/send_request_count" => {
                    subscription_metrics.message_count = latest_value;
                }
                "pubsub.googleapis.com/subscription/num_undelivered_messages" => {
                    subscription_metrics.undelivered_messages = latest_value;
                }
                "pubsub.googleapis.com/subscription/oldest_unacked_message_age" => {
                    subscription_metrics.oldest_unacked_message_age = latest_value;
                }
                "pubsub.googleapis.com/subscription/pull_request_count" => {
                    subscription_metrics.pull_operations = latest_value;
                }
                _ => {
                    warn!("Unknown metric type: {}", metric_type);
                }
            }
        }
        
        Ok(subscription_metrics)
    }

    /// Collect metrics for all topics and subscriptions
    pub async fn collect_all_metrics(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<PubSubMetrics>> {
        let topics = self.list_topics().await?;
        let subscriptions = self.list_subscriptions().await?;
        
        let mut tasks = Vec::new();
        
        // Collect topic metrics
        for topic_id in topics {
            let collector = self.clone();
            let topic_id_clone = topic_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_topic_metrics(&topic_id_clone, start_time, end_time).await
            }));
        }
        
        // Collect subscription metrics
        for subscription_id in subscriptions {
            let collector = self.clone();
            let subscription_id_clone = subscription_id.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_subscription_metrics(&subscription_id_clone, start_time, end_time).await
            }));
        }
        
        let mut all_metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => all_metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect Pub/Sub metrics: {}", e);
                }
            }
        }
        
        Ok(all_metrics)
    }
}

impl Clone for PubSubCollector {
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

/// Convert Pub/Sub metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn pubsub_metrics_to_reiver_format(
    metrics: &PubSubMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("resource_id:{}", metrics.resource_id),
        format!("resource_name:{}", metrics.resource_name),
        format!("resource_type:{}", metrics.resource_type),
        "source:gcp_pubsub".to_string(),
    ];

    if let Some(value) = metrics.message_count {
        let metric_name = if metrics.resource_type == "topic" {
            "gcp.pubsub.topic.message_count"
        } else {
            "gcp.pubsub.subscription.message_count"
        };
        reiver_metrics.push(ReiverMetric {
            name: metric_name.to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.message_bytes {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.pubsub.topic.message_bytes".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.undelivered_messages {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.pubsub.subscription.undelivered_messages".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.oldest_unacked_message_age {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.pubsub.subscription.oldest_unacked_message_age".to_string(),
            value,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.send_operations {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.pubsub.{}.send_operations", metrics.resource_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.pull_operations {
        reiver_metrics.push(ReiverMetric {
            name: "gcp.pubsub.subscription.pull_operations".to_string(),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    if let Some(value) = metrics.errors {
        reiver_metrics.push(ReiverMetric {
            name: format!("gcp.pubsub.{}.errors", metrics.resource_type),
            value,
            r#type: "counter".to_string(),
            timestamp: metrics.timestamp,
            tags: tags.clone(),
        });
    }

    reiver_metrics
}
