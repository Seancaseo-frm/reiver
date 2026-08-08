//! AWS integrations for Reiver
//!
//! This crate provides server-side integrations for AWS services.
//! Currently supports:
//! - EC2 (CloudWatch metrics collection)
//! - Lambda (CloudWatch metrics collection)
//! - S3 (CloudWatch metrics collection)
//! - RDS (CloudWatch metrics collection)
//! - DynamoDB (CloudWatch metrics collection)
//! - ElastiCache (CloudWatch metrics collection)
//! - ECS (CloudWatch metrics collection)
//! - EKS (CloudWatch metrics collection)
//! - SQS (CloudWatch metrics collection)
//! - CloudTrail (audit logs collection)
//! - IAM Access Analyzer (security findings collection)

pub mod ec2;
pub mod lambda;
pub mod s3;
pub mod rds;
pub mod redshift;
pub mod dynamodb;
pub mod elasticache;
pub mod ecs;
pub mod eks;
pub mod sqs;
pub mod sns;
pub mod kinesis;
pub mod apigateway;
pub mod cloudfront;
pub mod route53;
pub mod cloudtrail;
pub mod iam_access_analyzer;
pub mod config;

pub use config::AwsConfig;
pub use ec2::{Ec2Collector, Ec2Metrics, Ec2InstanceId, ec2_metrics_to_reiver_format};
pub use lambda::{LambdaCollector, LambdaMetrics, LambdaFunctionName, lambda_metrics_to_reiver_format};
pub use s3::{S3Collector, S3Metrics, S3BucketName, s3_metrics_to_reiver_format, ReiverMetric as S3ReiverMetric};
pub use rds::{RdsCollector, RdsMetrics, RdsInstanceId, rds_metrics_to_reiver_format, ReiverMetric as RdsReiverMetric};
pub use redshift::{RedshiftCollector, RedshiftMetrics, RedshiftClusterId, redshift_metrics_to_reiver_format, ReiverMetric as RedshiftReiverMetric};
pub use dynamodb::{DynamoDbCollector, DynamoDbMetrics, DynamoDbTableName, dynamodb_metrics_to_reiver_format, ReiverMetric as DynamoDbReiverMetric};
pub use elasticache::{ElastiCacheCollector, ElastiCacheMetrics, ElastiCacheClusterId, elasticache_metrics_to_reiver_format, ReiverMetric as ElastiCacheReiverMetric};
pub use ecs::{EcsCollector, EcsClusterMetrics, EcsServiceMetrics, EcsClusterName, EcsServiceId, ecs_cluster_metrics_to_reiver_format, ecs_service_metrics_to_reiver_format, ReiverMetric as EcsReiverMetric};
pub use eks::{EksCollector, EksClusterMetrics, EksClusterName, eks_metrics_to_reiver_format, ReiverMetric as EksReiverMetric};
pub use sqs::{SqsCollector, SqsMetrics, SqsQueueUrl, sqs_metrics_to_reiver_format, ReiverMetric as SqsReiverMetric};
pub use sns::{SnsCollector, SnsMetrics, SnsTopicArn, sns_metrics_to_reiver_format, ReiverMetric as SnsReiverMetric};
pub use kinesis::{KinesisCollector, KinesisMetrics, KinesisStreamName, kinesis_metrics_to_reiver_format, ReiverMetric as KinesisReiverMetric};
pub use apigateway::{ApiGatewayCollector, ApiGatewayMetrics, ApiGatewayRestApiId, ApiGatewayStage, apigateway_metrics_to_reiver_format, ReiverMetric as ApiGatewayReiverMetric};
pub use cloudfront::{CloudFrontCollector, CloudFrontMetrics, CloudFrontDistributionId, cloudfront_metrics_to_reiver_format, ReiverMetric as CloudFrontReiverMetric};
pub use route53::{Route53Collector, Route53HostedZoneMetrics, Route53HealthCheckMetrics, Route53HostedZoneId, Route53HealthCheckId, route53_hosted_zone_metrics_to_reiver_format, route53_health_check_metrics_to_reiver_format, ReiverMetric as Route53ReiverMetric};
pub use cloudtrail::{CloudTrailCollector, CloudTrailEvent, cloudtrail_event_to_log_message};
pub use iam_access_analyzer::{IamAccessAnalyzerCollector, IamAccessAnalyzerMetrics, AccessAnalyzerArn, iam_access_analyzer_metrics_to_reiver_format, ReiverMetric as IamAccessAnalyzerReiverMetric};

