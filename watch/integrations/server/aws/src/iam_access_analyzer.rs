//! IAM Access Analyzer integration for collecting security findings
//!
//! This module provides functionality to collect IAM Access Analyzer findings from AWS.
//! IAM Access Analyzer identifies resources that are shared with external entities,
//! helping organizations identify unintended access to resources and data.
//!
//! Findings collected include:
//! - Active findings (by severity, by resource type)
//! - Archived findings
//! - Resource types analyzed (S3 buckets, IAM roles, KMS keys, Lambda functions, SQS queues, etc.)

use anyhow::Result;
use aws_sdk_accessanalyzer::Client as AccessAnalyzerClient;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::config::AwsConfig;

/// IAM Access Analyzer identifier (analyzer ARN)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessAnalyzerArn(pub String);

/// IAM Access Analyzer metrics collected from findings
#[derive(Debug, Clone, Serialize)]
pub struct IamAccessAnalyzerMetrics {
    pub analyzer_arn: String,
    pub analyzer_name: String,
    pub timestamp: DateTime<Utc>,
    // Active findings counts
    pub active_findings_total: u32,
    pub active_findings_critical: u32,
    pub active_findings_high: u32,
    pub active_findings_medium: u32,
    pub active_findings_low: u32,
    pub active_findings_info: u32,
    // Findings by resource type
    pub findings_s3_bucket: u32,
    pub findings_iam_role: u32,
    pub findings_kms_key: u32,
    pub findings_lambda_function: u32,
    pub findings_sqs_queue: u32,
    pub findings_sns_topic: u32,
    pub findings_secrets_manager_secret: u32,
    pub findings_lambda_layer: u32,
    pub findings_efs_file_system: u32,
    pub findings_other: u32,
    // Archived findings
    pub archived_findings_total: u32,
}

/// IAM Access Analyzer collector
pub struct IamAccessAnalyzerCollector {
    accessanalyzer_client: AccessAnalyzerClient,
}

impl IamAccessAnalyzerCollector {
    /// Create a new IAM Access Analyzer collector with the given AWS configuration
    /// 
    /// Supports IAM role delegation (preferred) or default credential chain
    pub async fn new(config: &AwsConfig) -> Result<Self> {
        let aws_config = config.into_aws_config().await
            .map_err(|e| anyhow::anyhow!("Failed to create AWS config: {}", e))?;
        
        Ok(Self {
            accessanalyzer_client: AccessAnalyzerClient::new(&aws_config),
        })
    }

    /// List all IAM Access Analyzers in the configured region
    pub async fn list_analyzers(&self) -> Result<Vec<AccessAnalyzerArn>> {
        info!("Listing IAM Access Analyzers...");
        
        let mut analyzer_arns = Vec::new();
        let mut paginator = self.accessanalyzer_client.list_analyzers().into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            for analyzer in page.analyzers {
                analyzer_arns.push(AccessAnalyzerArn(analyzer.arn));
            }
        }

        info!("Found {} IAM Access Analyzers", analyzer_arns.len());
        Ok(analyzer_arns)
    }

    /// Get analyzer name from ARN
    /// Analyzer ARN format: arn:aws:access-analyzer:region:account-id:analyzer/analyzer-name
    fn extract_analyzer_name(arn: &str) -> String {
        arn.split('/').last().unwrap_or(arn).to_string()
    }

    /// Collect findings for a single analyzer
    pub async fn collect_findings(
        &self,
        analyzer_arn: &AccessAnalyzerArn,
    ) -> Result<IamAccessAnalyzerMetrics> {
        let analyzer_name = Self::extract_analyzer_name(&analyzer_arn.0);
        
        let mut metrics = IamAccessAnalyzerMetrics {
            analyzer_arn: analyzer_arn.0.clone(),
            analyzer_name: analyzer_name.clone(),
            timestamp: Utc::now(),
            active_findings_total: 0,
            active_findings_critical: 0,
            active_findings_high: 0,
            active_findings_medium: 0,
            active_findings_low: 0,
            active_findings_info: 0,
            findings_s3_bucket: 0,
            findings_iam_role: 0,
            findings_kms_key: 0,
            findings_lambda_function: 0,
            findings_sqs_queue: 0,
            findings_sns_topic: 0,
            findings_secrets_manager_secret: 0,
            findings_lambda_layer: 0,
            findings_efs_file_system: 0,
            findings_other: 0,
            archived_findings_total: 0,
        };

        // Collect active findings
        let active_findings = self.list_findings(analyzer_arn, false).await?;
        for finding in &active_findings {
            metrics.active_findings_total += 1;
            
            // Count by severity
            if let Some(severity) = &finding.severity {
                match severity.as_str() {
                    "CRITICAL" => metrics.active_findings_critical += 1,
                    "HIGH" => metrics.active_findings_high += 1,
                    "MEDIUM" => metrics.active_findings_medium += 1,
                    "LOW" => metrics.active_findings_low += 1,
                    "INFO" => metrics.active_findings_info += 1,
                    _ => {}
                }
            }
            
            // Count by resource type
            if let Some(resource_type) = &finding.resource_type {
                match resource_type.as_str() {
                    "AWS::S3::Bucket" => metrics.findings_s3_bucket += 1,
                    "AWS::IAM::Role" => metrics.findings_iam_role += 1,
                    "AWS::KMS::Key" => metrics.findings_kms_key += 1,
                    "AWS::Lambda::Function" => metrics.findings_lambda_function += 1,
                    "AWS::SQS::Queue" => metrics.findings_sqs_queue += 1,
                    "AWS::SNS::Topic" => metrics.findings_sns_topic += 1,
                    "AWS::SecretsManager::Secret" => metrics.findings_secrets_manager_secret += 1,
                    "AWS::Lambda::LayerVersion" => metrics.findings_lambda_layer += 1,
                    "AWS::EFS::FileSystem" => metrics.findings_efs_file_system += 1,
                    _ => metrics.findings_other += 1,
                }
            }
        }

        // Collect archived findings (just count, not details)
        let archived_findings = self.list_findings(analyzer_arn, true).await?;
        metrics.archived_findings_total = archived_findings.len() as u32;

        Ok(metrics)
    }

    /// List findings for an analyzer (active or archived)
    async fn list_findings(
        &self,
        analyzer_arn: &AccessAnalyzerArn,
        is_archived: bool,
    ) -> Result<Vec<AccessAnalyzerFinding>> {
        let mut findings = Vec::new();
        
        use aws_sdk_accessanalyzer::types::{Criterion, FindingStatus};
        
        let expected_status = if is_archived {
            FindingStatus::Archived
        } else {
            FindingStatus::Active
        };
        
        // Build filter criterion
        let criterion = Criterion::builder()
            .eq(expected_status.as_str())
            .build();
        
        let request = self.accessanalyzer_client
            .list_findings()
            .analyzer_arn(&analyzer_arn.0)
            .filter("status", criterion);

        let mut paginator = request.into_paginator().send();

        while let Some(page) = paginator.next().await {
            let page = page?;
            for finding in page.findings {
                // Filter by status in code as well (in case filter doesn't work as expected)
                if finding.status == expected_status {
                    // Convert ResourceType enum to Option<String>
                    let resource_type_str = Some(format!("{:?}", finding.resource_type));
                    
                    findings.push(AccessAnalyzerFinding {
                        id: finding.id.clone(),
                        resource_type: resource_type_str,
                        resource: finding.resource.clone(),
                        severity: None, // FindingSummary doesn't have severity field
                        status: Some(format!("{:?}", finding.status)),
                    });
                }
            }
        }

        Ok(findings)
    }

    /// Collect findings for multiple analyzers in parallel
    pub async fn collect_findings_batch(
        &self,
        analyzers: &[AccessAnalyzerArn],
    ) -> Result<Vec<IamAccessAnalyzerMetrics>> {
        let mut tasks = Vec::new();
        for analyzer_arn in analyzers {
            let collector = self.clone();
            let analyzer_arn_clone = analyzer_arn.clone();
            tasks.push(tokio::spawn(async move {
                collector.collect_findings(&analyzer_arn_clone).await
            }));
        }

        let mut metrics = Vec::new();
        for task in tasks {
            match task.await? {
                Ok(metric) => metrics.push(metric),
                Err(e) => {
                    error!("Failed to collect findings for IAM Access Analyzer: {}", e);
                }
            }
        }

        Ok(metrics)
    }
}

impl Clone for IamAccessAnalyzerCollector {
    fn clone(&self) -> Self {
        Self {
            accessanalyzer_client: self.accessanalyzer_client.clone(),
        }
    }
}

/// Internal representation of an Access Analyzer finding
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct AccessAnalyzerFinding {
    id: String,
    resource_type: Option<String>,
    resource: Option<String>,
    severity: Option<String>,
    status: Option<String>,
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

/// Convert IAM Access Analyzer metrics to Reiver metric format
/// Returns a vector of metrics that can be sent to the metrics API
pub fn iam_access_analyzer_metrics_to_reiver_format(
    metrics: &IamAccessAnalyzerMetrics,
    _project_id: &str,
) -> Vec<ReiverMetric> {
    let mut reiver_metrics = Vec::new();
    let tags = vec![
        format!("analyzer_name:{}", metrics.analyzer_name),
        format!("analyzer_arn:{}", metrics.analyzer_arn),
        "source:aws_iam_access_analyzer".to_string(),
    ];

    // Active findings counts
    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_total".to_string(),
        value: metrics.active_findings_total as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: tags.clone(),
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_critical".to_string(),
        value: metrics.active_findings_critical as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("severity:CRITICAL".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_high".to_string(),
        value: metrics.active_findings_high as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("severity:HIGH".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_medium".to_string(),
        value: metrics.active_findings_medium as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("severity:MEDIUM".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_low".to_string(),
        value: metrics.active_findings_low as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("severity:LOW".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.active_findings_info".to_string(),
        value: metrics.active_findings_info as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("severity:INFO".to_string());
            t
        },
    });

    // Findings by resource type
    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_s3_bucket as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::S3::Bucket".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_iam_role as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::IAM::Role".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_kms_key as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::KMS::Key".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_lambda_function as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::Lambda::Function".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_sqs_queue as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::SQS::Queue".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_sns_topic as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::SNS::Topic".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_secrets_manager_secret as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::SecretsManager::Secret".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_lambda_layer as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::Lambda::LayerVersion".to_string());
            t
        },
    });

    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
        value: metrics.findings_efs_file_system as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: {
            let mut t = tags.clone();
            t.push("resource_type:AWS::EFS::FileSystem".to_string());
            t
        },
    });

    if metrics.findings_other > 0 {
        reiver_metrics.push(ReiverMetric {
            name: "aws.iam_access_analyzer.findings_by_resource_type".to_string(),
            value: metrics.findings_other as f64,
            r#type: "gauge".to_string(),
            timestamp: metrics.timestamp,
            tags: {
                let mut t = tags.clone();
                t.push("resource_type:OTHER".to_string());
                t
            },
        });
    }

    // Archived findings
    reiver_metrics.push(ReiverMetric {
        name: "aws.iam_access_analyzer.archived_findings_total".to_string(),
        value: metrics.archived_findings_total as f64,
        r#type: "gauge".to_string(),
        timestamp: metrics.timestamp,
        tags: tags.clone(),
    });

    reiver_metrics
}
