//! CloudTrail integration for collecting audit logs
//!
//! This module provides functionality to collect CloudTrail events (audit logs) from AWS CloudTrail API.
//! CloudTrail logs AWS API calls and actions, providing audit trails for security and compliance.
//!
//! Events collected include:
//! - API calls (who, what, when, where)
//! - Resource changes
//! - Management events
//! - Data events (optional, configurable)

use anyhow::Result;
use aws_sdk_cloudtrail::Client as CloudTrailClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::AwsConfig;

/// CloudTrail event (audit log entry)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTrailEvent {
    pub event_time: DateTime<Utc>,
    pub event_name: String,
    pub username: Option<String>,
    pub resource_type: Option<String>,
    pub resource_name: Option<String>,
    pub event_source: Option<String>,
    pub aws_region: Option<String>,
    pub source_ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub request_parameters: Option<serde_json::Value>,
    pub response_elements: Option<serde_json::Value>,
    pub additional_event_data: Option<serde_json::Value>,
    pub event_id: String,
    pub read_only: Option<bool>,
    pub resources: Option<Vec<CloudTrailResource>>,
    pub event_type: Option<String>,
}

/// CloudTrail resource referenced in an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTrailResource {
    pub resource_type: Option<String>,
    pub resource_name: Option<String>,
}

/// CloudTrail events collector
pub struct CloudTrailCollector {
    cloudtrail_client: CloudTrailClient,
}

impl CloudTrailCollector {
    /// Create a new CloudTrail collector with the given AWS configuration
    /// 
    /// Supports both IAM role delegation (preferred) and access keys (legacy)
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            cloudtrail_client: CloudTrailClient::new(&aws_config),
        })
    }

    /// Lookup CloudTrail events for a given time range
    /// 
    /// CloudTrail events are stored for 90 days in the CloudTrail event history.
    /// This method queries the event history for the specified time range.
    pub async fn lookup_events(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        max_results: Option<i32>,
    ) -> Result<Vec<CloudTrailEvent>> {
        info!("Looking up CloudTrail events from {} to {}", start_time, end_time);

        let mut events = Vec::new();
        
        // Convert chrono DateTime to AWS SDK DateTime
        use aws_sdk_cloudtrail::primitives::DateTime as AwsDateTime;
        let start_aws = AwsDateTime::from_millis(start_time.timestamp_millis());
        let end_aws = AwsDateTime::from_millis(end_time.timestamp_millis());

        let mut request = self.cloudtrail_client
            .lookup_events()
            .start_time(start_aws)
            .end_time(end_aws);

        if let Some(max) = max_results {
            request = request.max_results(max);
        }

        let mut paginator = request.into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(aws_events) = page.events {
                for aws_event in aws_events {
                    match self.convert_cloudtrail_event(aws_event).await {
                        Ok(event) => events.push(event),
                        Err(e) => {
                            warn!("Failed to convert CloudTrail event: {}", e);
                        }
                    }
                }
            }
        }

        info!("Found {} CloudTrail events", events.len());
        Ok(events)
    }

    /// Convert AWS SDK CloudTrail event to our CloudTrailEvent format
    async fn convert_cloudtrail_event(
        &self,
        aws_event: aws_sdk_cloudtrail::types::Event,
    ) -> Result<CloudTrailEvent> {
        // Parse the CloudTrail event JSON (CloudTrail events are stored as JSON strings)
        let event_string = aws_event.cloud_trail_event
            .ok_or_else(|| anyhow::anyhow!("CloudTrail event missing cloud_trail_event field"))?;

        // Parse the JSON event
        let event_json: serde_json::Value = serde_json::from_str(&event_string)
            .map_err(|e| anyhow::anyhow!("Failed to parse CloudTrail event JSON: {}", e))?;

        // Extract fields from the JSON event
        let event_time_str = event_json.get("eventTime")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing eventTime in CloudTrail event"))?;
        
        let event_time = DateTime::parse_from_rfc3339(event_time_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse eventTime: {}", e))?
            .with_timezone(&Utc);

        let event_name = event_json.get("eventName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing eventName in CloudTrail event"))?;

        let username = event_json.get("userIdentity")
            .and_then(|v| v.get("userName"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                event_json.get("userIdentity")
                    .and_then(|v| v.get("type"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });

        let resources = event_json.get("resources")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| {
                        Some(CloudTrailResource {
                            resource_type: r.get("resourceType")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            resource_name: r.get("resourceName")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                        })
                    })
                    .collect()
            });

        // Get the first resource for backward compatibility fields
        let resource: Option<&CloudTrailResource> = resources.as_ref()
            .and_then(|r: &Vec<CloudTrailResource>| r.first());
        
        let event = CloudTrailEvent {
            event_time,
            event_name: event_name.clone(),
            username,
            resource_type: resource.and_then(|r| r.resource_type.clone()),
            resource_name: resource.and_then(|r| r.resource_name.clone()),
            event_source: event_json.get("eventSource")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            aws_region: event_json.get("awsRegion")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            source_ip_address: event_json.get("sourceIPAddress")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            user_agent: event_json.get("userAgent")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error_code: event_json.get("errorCode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error_message: event_json.get("errorMessage")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            request_parameters: event_json.get("requestParameters").cloned(),
            response_elements: event_json.get("responseElements").cloned(),
            additional_event_data: event_json.get("additionalEventData").cloned(),
            event_id: aws_event.event_id
                .unwrap_or_else(|| format!("event-{}", uuid::Uuid::new_v4())),
            read_only: event_json.get("readOnly")
                .and_then(|v| v.as_bool()),
            resources,
            event_type: event_json.get("eventType")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        Ok(event)
    }
}

/// Convert CloudTrail event to log message format for storage
/// 
/// CloudTrail events are stored as logs with structured metadata in tags
pub fn cloudtrail_event_to_log_message(event: &CloudTrailEvent) -> (String, String, Vec<String>) {
    // Create a human-readable log message
    let mut message_parts = Vec::new();
    
    message_parts.push(format!("Event: {}", event.event_name));
    
    if let Some(username) = &event.username {
        message_parts.push(format!("User: {}", username));
    }
    
    if let Some(resource_type) = &event.resource_type {
        message_parts.push(format!("Resource: {}", resource_type));
    }
    
    if let Some(resource_name) = &event.resource_name {
        message_parts.push(format!("ResourceName: {}", resource_name));
    }
    
    if let Some(error_code) = &event.error_code {
        message_parts.push(format!("Error: {} ({})", error_code, event.error_message.as_deref().unwrap_or("")));
    }
    
    let message = message_parts.join(" | ");
    
    // Determine log level based on error_code
    let level = if event.error_code.is_some() {
        "error"
    } else {
        "info"
    };
    
    // Create tags for structured querying
    let mut tags = Vec::new();
    tags.push(format!("event_name:{}", event.event_name));
    
    if let Some(username) = &event.username {
        tags.push(format!("username:{}", username));
    }
    
    if let Some(resource_type) = &event.resource_type {
        tags.push(format!("resource_type:{}", resource_type));
    }
    
    if let Some(event_source) = &event.event_source {
        tags.push(format!("event_source:{}", event_source));
    }
    
    if let Some(aws_region) = &event.aws_region {
        tags.push(format!("aws_region:{}", aws_region));
    }
    
    if let Some(error_code) = &event.error_code {
        tags.push(format!("error_code:{}", error_code));
    }
    
    if let Some(read_only) = event.read_only {
        tags.push(format!("read_only:{}", read_only));
    }
    
    if let Some(event_type) = &event.event_type {
        tags.push(format!("event_type:{}", event_type));
    }
    
    tags.push("source:aws_cloudtrail".to_string());
    
    (message, level.to_string(), tags)
}

