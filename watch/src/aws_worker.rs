//! AWS integrations background worker
//!
//! Polls AWS services (EC2, Lambda, etc.) for metrics and stores them in ClickHouse

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;
use crate::db::DbPool;
use std::collections::HashMap;
use parking_lot::Mutex;
use reiver_integrations_server_aws::{
    ec2,
    lambda,
    s3,
    rds,
    redshift,
    dynamodb,
    elasticache,
    ecs,
    eks,
    sqs,
    sns,
    kinesis,
    apigateway,
    cloudfront,
    route53,
    cloudtrail,
    iam_access_analyzer,
    config::AwsConfig,
};

/// Database row structure for AWS integration configs
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct AwsIntegrationConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    region: String,
    // IAM Role Delegation (preferred)
    role_arn: Option<String>,
    external_id: Option<String>,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Tracks last collection time per integration config ID
static LAST_COLLECTION_TIMES: once_cell::sync::Lazy<Mutex<HashMap<Uuid, DateTime<Utc>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Start AWS integrations worker
/// Polls AWS integration configs from database and collects metrics
pub async fn start_aws_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting AWS integrations worker...");

    let handle = tokio::spawn(async move {
        // Check every 10 seconds for configs that need collection
        let mut interval = tokio::time::interval(StdDuration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = collect_all_aws_metrics(&db_pool, &clickhouse_pool).await {
                        error!("AWS worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("AWS worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("AWS worker stopped");
    });

    Ok(handle)
}

/// Collect metrics from all enabled AWS integrations
async fn collect_all_aws_metrics(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    // Fetch all enabled AWS integration configs
    let configs: Vec<AwsIntegrationConfigRow> = sqlx::query_as::<_, AwsIntegrationConfigRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            region,
            role_arn,
            external_id,
            enabled,
            collection_interval_seconds,
            config_jsonb
        FROM aws_integration_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to fetch AWS integration configs: {}", e))?;

    info!("Found {} enabled AWS integrations", configs.len());

    // Clean up stale entries from LAST_COLLECTION_TIMES for deleted configs
    {
        let config_ids: std::collections::HashSet<Uuid> = configs.iter().map(|c| c.id).collect();
        let mut last_times = LAST_COLLECTION_TIMES.lock();
        last_times.retain(|id, _| config_ids.contains(id));
    }

    for config in configs {
        // Check if it's time to collect based on collection_interval_seconds
        let now = Utc::now();
        let should_collect = {
            let last_times = LAST_COLLECTION_TIMES.lock();
            match last_times.get(&config.id) {
                Some(last_time) => {
                    let elapsed = now.signed_duration_since(*last_time);
                    elapsed.num_seconds() >= config.collection_interval_seconds as i64
                }
                None => true, // Never collected, should collect now
            }
        };

        if !should_collect {
            continue;
        }

        // Update last collection time before collecting (to prevent concurrent collections)
        {
            let mut last_times = LAST_COLLECTION_TIMES.lock();
            last_times.insert(config.id, now);
        }

        match config.integration_type.as_str() {
            "ec2" => {
                if let Err(e) = collect_ec2_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect EC2 metrics for integration {}: {}", config.id, e);
                }
            }
            "lambda" => {
                if let Err(e) = collect_lambda_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Lambda metrics for integration {}: {}", config.id, e);
                }
            }
            "s3" => {
                if let Err(e) = collect_s3_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect S3 metrics for integration {}: {}", config.id, e);
                }
            }
            "rds" => {
                if let Err(e) = collect_rds_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect RDS metrics for integration {}: {}", config.id, e);
                }
            }
            "redshift" => {
                if let Err(e) = collect_redshift_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Redshift metrics for integration {}: {}", config.id, e);
                }
            }
            "dynamodb" => {
                if let Err(e) = collect_dynamodb_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect DynamoDB metrics for integration {}: {}", config.id, e);
                }
            }
            "elasticache" => {
                if let Err(e) = collect_elasticache_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect ElastiCache metrics for integration {}: {}", config.id, e);
                }
            }
            "ecs" => {
                if let Err(e) = collect_ecs_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect ECS metrics for integration {}: {}", config.id, e);
                }
            }
            "eks" => {
                if let Err(e) = collect_eks_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect EKS metrics for integration {}: {}", config.id, e);
                }
            }
            "sqs" => {
                if let Err(e) = collect_sqs_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect SQS metrics for integration {}: {}", config.id, e);
                }
            }
            "kinesis" => {
                if let Err(e) = collect_kinesis_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Kinesis metrics for integration {}: {}", config.id, e);
                }
            }
            "apigateway" => {
                if let Err(e) = collect_apigateway_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect API Gateway metrics for integration {}: {}", config.id, e);
                }
            }
            "cloudfront" => {
                if let Err(e) = collect_cloudfront_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect CloudFront metrics for integration {}: {}", config.id, e);
                }
            }
            "cloudtrail" => {
                if let Err(e) = collect_cloudtrail_events(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect CloudTrail events for integration {}: {}", config.id, e);
                }
            }
            "iam_access_analyzer" => {
                if let Err(e) = collect_iam_access_analyzer_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect IAM Access Analyzer metrics for integration {}: {}", config.id, e);
                }
            }
            "sns" => {
                if let Err(e) = collect_sns_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect SNS metrics for integration {}: {}", config.id, e);
                }
            }
            "route53" => {
                if let Err(e) = collect_route53_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Route53 metrics for integration {}: {}", config.id, e);
                }
            }
            _ => {
                warn!("Unsupported AWS integration type: {}", config.integration_type);
            }
        }
    }

    Ok(())
}

/// Collect EC2 metrics for a specific integration config
async fn collect_ec2_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting EC2 metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create EC2 collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = ec2::Ec2Collector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create EC2 collector: {}", e))?;

    // List instances
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list EC2 instances: {}", e))?;

    if instances.is_empty() {
        info!("No EC2 instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect EC2 metrics: {}", e))?;

    info!("Collected {} EC2 metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<ec2::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = ec2::ec2_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_ec2_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store EC2 metrics: {}", e))?;

    info!("Stored {} EC2 metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect Lambda metrics for a specific integration config
async fn collect_lambda_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Lambda metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create Lambda collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = lambda::LambdaCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create Lambda collector: {}", e))?;

    // List functions
    let functions = collector.list_functions().await
        .map_err(|e| anyhow::anyhow!("Failed to list Lambda functions: {}", e))?;

    if functions.is_empty() {
        info!("No Lambda functions found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all functions
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&functions, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Lambda metrics: {}", e))?;

    info!("Collected {} Lambda metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<lambda::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = lambda::lambda_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_lambda_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Lambda metrics: {}", e))?;

    info!("Stored {} Lambda metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect S3 metrics for a specific integration config
async fn collect_s3_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting S3 metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create S3 collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = s3::S3Collector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create S3 collector: {}", e))?;

    // List buckets
    let buckets = collector.list_buckets().await
        .map_err(|e| anyhow::anyhow!("Failed to list S3 buckets: {}", e))?;

    if buckets.is_empty() {
        info!("No S3 buckets found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all buckets
    // S3 metrics are reported daily, so collect last 7 days to get recent data
    let end_time = Utc::now();
    let start_time = end_time - Duration::days(7);

    let metrics = collector
        .collect_metrics_batch(&buckets, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect S3 metrics: {}", e))?;

    info!("Collected {} S3 metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<s3::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = s3::s3_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_s3_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store S3 metrics: {}", e))?;

    info!("Stored {} S3 metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store S3 metrics in ClickHouse (compatible with metrics API format)
async fn store_s3_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[s3::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect RDS metrics for a specific integration config
async fn collect_rds_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting RDS metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create RDS collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = rds::RdsCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create RDS collector: {}", e))?;

    // List instances (returns instance IDs and engine types)
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list RDS instances: {}", e))?;

    if instances.is_empty() {
        info!("No RDS instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect RDS metrics: {}", e))?;

    info!("Collected {} RDS metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<rds::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = rds::rds_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_rds_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store RDS metrics: {}", e))?;

    info!("Stored {} RDS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect Redshift metrics for a specific integration config
async fn collect_redshift_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Redshift metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
        role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create Redshift collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = redshift::RedshiftCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create Redshift collector: {}", e))?;

    // List clusters (returns cluster IDs and identifiers)
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list Redshift clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No Redshift clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Redshift metrics: {}", e))?;

    info!("Collected {} Redshift metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<redshift::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = redshift::redshift_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_redshift_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Redshift metrics: {}", e))?;

    info!("Stored {} Redshift metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Redshift metrics in ClickHouse (compatible with metrics API format)
async fn store_redshift_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[redshift::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write Redshift metric to ClickHouse: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit Redshift metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect DynamoDB metrics for a specific integration config
async fn collect_dynamodb_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting DynamoDB metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create DynamoDB collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = dynamodb::DynamoDbCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create DynamoDB collector: {}", e))?;

    // List tables
    let tables = collector.list_tables().await
        .map_err(|e| anyhow::anyhow!("Failed to list DynamoDB tables: {}", e))?;

    if tables.is_empty() {
        info!("No DynamoDB tables found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all tables
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&tables, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect DynamoDB metrics: {}", e))?;

    info!("Collected {} DynamoDB metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<dynamodb::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = dynamodb::dynamodb_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_dynamodb_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store DynamoDB metrics: {}", e))?;

    info!("Stored {} DynamoDB metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect ElastiCache metrics for a specific integration config
async fn collect_elasticache_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting ElastiCache metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create ElastiCache collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = elasticache::ElastiCacheCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create ElastiCache collector: {}", e))?;

    // List clusters (returns cluster IDs and engine types)
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list ElastiCache clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No ElastiCache clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect ElastiCache metrics: {}", e))?;

    info!("Collected {} ElastiCache metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<elasticache::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = elasticache::elasticache_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_elasticache_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store ElastiCache metrics: {}", e))?;

    info!("Stored {} ElastiCache metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store ElastiCache metrics in ClickHouse (compatible with metrics API format)
async fn store_elasticache_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[elasticache::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store DynamoDB metrics in ClickHouse (compatible with metrics API format)
async fn store_dynamodb_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[dynamodb::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect ECS metrics for a specific integration config
async fn collect_ecs_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting ECS metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create ECS collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = ecs::EcsCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create ECS collector: {}", e))?;

    // List clusters
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list ECS clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No ECS clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let cluster_metrics = collector
        .collect_cluster_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect ECS cluster metrics: {}", e))?;

    info!("Collected {} ECS cluster metric sets for integration {}", cluster_metrics.len(), config.id);

    // Collect service metrics for all clusters
    let mut all_service_metrics = Vec::new();
    for cluster in &clusters {
        match collector.list_services(&cluster.0).await {
            Ok(services) => {
                if !services.is_empty() {
                    match collector.collect_service_metrics_batch(&services, start_time, end_time).await {
                        Ok(service_metrics) => all_service_metrics.extend(service_metrics),
                        Err(e) => {
                            error!("Failed to collect service metrics for cluster {}: {}", cluster.0, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list services for cluster {}: {}", cluster.0, e);
            }
        }
    }

    info!("Collected {} ECS service metric sets for integration {}", all_service_metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<ecs::ReiverMetric> = Vec::new();

    // Add cluster metrics
    for metric in &cluster_metrics {
        let reiver_metrics = ecs::ecs_cluster_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    // Add service metrics
    for metric in &all_service_metrics {
        let reiver_metrics = ecs::ecs_service_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_ecs_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store ECS metrics: {}", e))?;

    info!("Stored {} ECS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store ECS metrics in ClickHouse (compatible with metrics API format)
async fn store_ecs_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[ecs::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect EKS metrics for a specific integration config
async fn collect_eks_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting EKS metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create EKS collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = eks::EksCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create EKS collector: {}", e))?;

    // List clusters
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list EKS clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No EKS clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect EKS metrics: {}", e))?;

    info!("Collected {} EKS cluster metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<eks::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = eks::eks_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_eks_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store EKS metrics: {}", e))?;

    info!("Stored {} EKS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store EKS metrics in ClickHouse (compatible with metrics API format)
async fn store_eks_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[eks::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect SQS metrics for a specific integration config
async fn collect_sqs_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting SQS metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create SQS collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = sqs::SqsCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create SQS collector: {}", e))?;

    // List queues
    let queues = collector.list_queues().await
        .map_err(|e| anyhow::anyhow!("Failed to list SQS queues: {}", e))?;

    if queues.is_empty() {
        info!("No SQS queues found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all queues
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&queues, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect SQS metrics: {}", e))?;

    info!("Collected {} SQS queue metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<sqs::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = sqs::sqs_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_sqs_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store SQS metrics: {}", e))?;

    info!("Stored {} SQS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store SQS metrics in ClickHouse (compatible with metrics API format)
async fn store_sqs_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[sqs::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect SNS metrics for a specific integration config
async fn collect_sns_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting SNS metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create SNS collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = sns::SnsCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create SNS collector: {}", e))?;

    // List topics
    let topics = collector.list_topics().await
        .map_err(|e| anyhow::anyhow!("Failed to list SNS topics: {}", e))?;

    if topics.is_empty() {
        info!("No SNS topics found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all topics
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&topics, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect SNS metrics: {}", e))?;

    info!("Collected {} SNS topic metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<sns::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = sns::sns_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_sns_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store SNS metrics: {}", e))?;

    info!("Stored {} SNS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store SNS metrics in ClickHouse (compatible with metrics API format)
async fn store_sns_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[sns::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect Kinesis metrics for a specific integration config
async fn collect_kinesis_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Kinesis metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create Kinesis collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = kinesis::KinesisCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create Kinesis collector: {}", e))?;

    // List streams
    let streams = collector.list_streams().await
        .map_err(|e| anyhow::anyhow!("Failed to list Kinesis streams: {}", e))?;

    if streams.is_empty() {
        info!("No Kinesis streams found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all streams
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&streams, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Kinesis metrics: {}", e))?;

    info!("Collected {} Kinesis stream metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<kinesis::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = kinesis::kinesis_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_kinesis_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Kinesis metrics: {}", e))?;

    info!("Stored {} Kinesis metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Kinesis metrics in ClickHouse (compatible with metrics API format)
async fn store_kinesis_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[kinesis::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect API Gateway metrics for a specific integration config
async fn collect_apigateway_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting API Gateway metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create API Gateway collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = apigateway::ApiGatewayCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create API Gateway collector: {}", e))?;

    // List REST APIs
    let rest_apis = collector.list_rest_apis().await
        .map_err(|e| anyhow::anyhow!("Failed to list API Gateway REST APIs: {}", e))?;

    if rest_apis.is_empty() {
        info!("No API Gateway REST APIs found for integration {}", config.id);
        return Ok(());
    }

    // Collect stages for each REST API
    let mut all_stages = Vec::new();
    for rest_api in &rest_apis {
        match collector.list_stages(&rest_api.0).await {
            Ok(stage_names) => {
                for stage_name in stage_names {
                    all_stages.push(apigateway::ApiGatewayStage {
                        rest_api_id: rest_api.0.clone(),
                        stage_name,
                    });
                }
            }
            Err(e) => {
                warn!("Failed to list stages for REST API {}: {}", rest_api.0, e);
            }
        }
    }

    if all_stages.is_empty() {
        info!("No API Gateway stages found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all stages
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&all_stages, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect API Gateway metrics: {}", e))?;

    info!("Collected {} API Gateway stage metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<apigateway::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = apigateway::apigateway_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_apigateway_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store API Gateway metrics: {}", e))?;

    info!("Stored {} API Gateway metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store API Gateway metrics in ClickHouse (compatible with metrics API format)
async fn store_apigateway_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[apigateway::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect CloudFront metrics for a specific integration config
async fn collect_cloudfront_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting CloudFront metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
        role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create CloudFront collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = cloudfront::CloudFrontCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create CloudFront collector: {}", e))?;

    // List distributions
    let distributions = collector.list_distributions().await
        .map_err(|e| anyhow::anyhow!("Failed to list CloudFront distributions: {}", e))?;

    if distributions.is_empty() {
        info!("No CloudFront distributions found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all distributions
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&distributions, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect CloudFront metrics: {}", e))?;

    info!("Collected {} CloudFront distribution metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cloudfront::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cloudfront::cloudfront_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_cloudfront_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store CloudFront metrics: {}", e))?;

    info!("Stored {} CloudFront metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store CloudFront metrics in ClickHouse (compatible with metrics API format)
async fn store_cloudfront_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cloudfront::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect Route53 metrics for a specific integration config
async fn collect_route53_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Route53 metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
        role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create Route53 collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = route53::Route53Collector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create Route53 collector: {}", e))?;

    // List hosted zones and health checks
    let hosted_zones = collector.list_hosted_zones().await
        .map_err(|e| anyhow::anyhow!("Failed to list Route53 hosted zones: {}", e))?;

    let health_checks = collector.list_health_checks().await
        .map_err(|e| anyhow::anyhow!("Failed to list Route53 health checks: {}", e))?;

    if hosted_zones.is_empty() && health_checks.is_empty() {
        info!("No Route53 hosted zones or health checks found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all hosted zones and health checks
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let mut all_reiver_metrics: Vec<route53::ReiverMetric> = Vec::new();

    // Collect hosted zone metrics
    if !hosted_zones.is_empty() {
        let hosted_zone_metrics = collector
            .collect_hosted_zone_metrics_batch(&hosted_zones, start_time, end_time)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect Route53 hosted zone metrics: {}", e))?;

        info!("Collected {} Route53 hosted zone metric sets for integration {}", hosted_zone_metrics.len(), config.id);

        let project_id_str = config.project_id.to_string();
        for metric in &hosted_zone_metrics {
            let reiver_metrics = route53::route53_hosted_zone_metrics_to_reiver_format(metric, &project_id_str);
            all_reiver_metrics.extend(reiver_metrics);
        }
    }

    // Collect health check metrics
    if !health_checks.is_empty() {
        let health_check_metrics = collector
            .collect_health_check_metrics_batch(&health_checks, start_time, end_time)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to collect Route53 health check metrics: {}", e))?;

        info!("Collected {} Route53 health check metric sets for integration {}", health_check_metrics.len(), config.id);

        let project_id_str = config.project_id.to_string();
        for metric in &health_check_metrics {
            let reiver_metrics = route53::route53_health_check_metrics_to_reiver_format(metric, &project_id_str);
            all_reiver_metrics.extend(reiver_metrics);
        }
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_route53_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Route53 metrics: {}", e))?;

    info!("Stored {} Route53 metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Route53 metrics in ClickHouse (compatible with metrics API format)
async fn store_route53_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[route53::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Collect CloudTrail events for a specific integration config
async fn collect_cloudtrail_events(
    config: &AwsIntegrationConfigRow,
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting CloudTrail events for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
                role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create CloudTrail collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = cloudtrail::CloudTrailCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create CloudTrail collector: {}", e))?;

    // Collect events from the last hour (CloudTrail events are available for 90 days)
    // Adjust time range based on collection interval
    let end_time = Utc::now();
    let start_time = end_time - Duration::hours(1); // Collect last hour of events

    let events = collector.lookup_events(start_time, end_time, Some(1000)).await
        .map_err(|e| anyhow::anyhow!("Failed to lookup CloudTrail events: {}", e))?;

    if events.is_empty() {
        info!("No CloudTrail events found for integration {}", config.id);
        return Ok(());
    }

    info!("Collected {} CloudTrail events for integration {}", events.len(), config.id);

    // Store events as logs in ClickHouse
    store_cloudtrail_events_in_clickhouse(db_pool, clickhouse_pool, config.project_id, &events).await
        .map_err(|e| anyhow::anyhow!("Failed to store CloudTrail events: {}", e))?;

    info!("Stored {} CloudTrail events for integration {}", events.len(), config.id);

    Ok(())
}

/// Store CloudTrail events in ClickHouse logs table (OTel format)
/// Transforms CloudTrail events to OTel semantic conventions
async fn store_cloudtrail_events_in_clickhouse(
    db: &DbPool,
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    events: &[cloudtrail::CloudTrailEvent],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    let pii_enabled = crate::pii::get_pii_masking_enabled(db, project_id).await;

    // OTel-compatible log insert struct (snake_case)
    #[derive(Row, Serialize)]
    struct LogInsert {
        project_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        trace_id: String,
        span_id: String,
        severity_text: String,
        severity_number: u8,
        service_name: String,
        body: String,
        resource_attributes: Vec<(String, String)>,
        log_attributes: Vec<(String, String)>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<LogInsert>("logs")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let project_id_str = project_id.to_string();

    for event in events {
        // Transform CloudTrail event to OTel log format
        let (body, severity_text, severity_number) = cloudtrail_to_otel_log(event);
        // mask_pii returns Cow<str> - only allocates when PII is found
        let body = if pii_enabled { crate::pii::mask_pii(&body).into_owned() } else { body };

        // Build resource attributes (OTel semantic conventions for AWS)
        let mut resource_attributes: Vec<(String, String)> = vec![
            ("cloud.provider".to_string(), "aws".to_string()),
            ("aws.log.group.names".to_string(), "cloudtrail".to_string()),
        ];
        
        if let Some(ref region) = event.aws_region {
            resource_attributes.push(("cloud.region".to_string(), region.clone()));
        }
        if let Some(ref username) = event.username {
            resource_attributes.push(("enduser.id".to_string(), username.clone()));
        }

        // Build log attributes (event-specific data)
        let mut log_attributes: Vec<(String, String)> = vec![
            ("aws.cloudtrail.event_name".to_string(), event.event_name.clone()),
            ("aws.cloudtrail.event_source".to_string(), event.event_source.clone().unwrap_or_default()),
            ("aws.cloudtrail.event_type".to_string(), event.event_type.clone().unwrap_or_default()),
            ("aws.cloudtrail.event_id".to_string(), event.event_id.clone()),
        ];
        
        if let Some(ref source_ip) = event.source_ip_address {
            log_attributes.push(("client.address".to_string(), source_ip.clone()));
        }
        if let Some(ref user_agent) = event.user_agent {
            log_attributes.push(("user_agent.original".to_string(), user_agent.clone()));
        }
        if let Some(ref error_code) = event.error_code {
            log_attributes.push(("aws.cloudtrail.error_code".to_string(), error_code.clone()));
        }
        if let Some(ref error_message) = event.error_message {
            log_attributes.push(("aws.cloudtrail.error_message".to_string(), error_message.clone()));
        }
        // Store request/response as JSON strings
        if let Some(ref params) = event.request_parameters {
            log_attributes.push(("aws.cloudtrail.request_parameters".to_string(), 
                serde_json::to_string(params).unwrap_or_default()));
        }
        if let Some(ref elements) = event.response_elements {
            log_attributes.push(("aws.cloudtrail.response_elements".to_string(), 
                serde_json::to_string(elements).unwrap_or_default()));
        }

        let log_row = LogInsert {
            project_id: project_id_str.clone(),
            timestamp: event.event_time,
            trace_id: String::new(),  // CloudTrail events don't have trace context
            span_id: String::new(),
            severity_text,
            severity_number,
            service_name: event.event_source.clone().unwrap_or_else(|| "aws.cloudtrail".to_string()),
            body,
            resource_attributes,
            log_attributes,
        };

        inserter.write(&log_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write CloudTrail event to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit CloudTrail events to ClickHouse: {}", e))?;
    
    inserter.end().await
        .map_err(|e| anyhow::anyhow!("Failed to end CloudTrail inserter: {}", e))?;

    Ok(())
}

/// Transform CloudTrail event to OTel log format
/// Returns (body, severity_text, severity_number)
fn cloudtrail_to_otel_log(event: &cloudtrail::CloudTrailEvent) -> (String, String, u8) {
    // Build descriptive body message
    let body = format!(
        "{} on {} by {}",
        event.event_name,
        event.event_source.clone().unwrap_or_else(|| "unknown".to_string()),
        event.username.clone().unwrap_or_else(|| "unknown".to_string())
    );

    // Determine severity based on error presence
    let (severity_text, severity_number) = if event.error_code.is_some() {
        ("ERROR".to_string(), 17u8)  // OTel ERROR = 17
    } else {
        ("INFO".to_string(), 9u8)    // OTel INFO = 9
    };

    (body, severity_text, severity_number)
}

/// Collect IAM Access Analyzer metrics for a specific integration config
async fn collect_iam_access_analyzer_metrics(
    config: &AwsIntegrationConfigRow,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting IAM Access Analyzer metrics for integration: {}", config.id);

    // Create AWS config from database config
    // Priority: IAM role (preferred) or default credential chain
    let aws_config = AwsConfig {
        region: config.region.clone(),
        role_arn: config.role_arn.clone(),
        external_id: config.external_id.clone(),
    };

    // Create IAM Access Analyzer collector
    // This will use IAM role delegation if role_arn is set, otherwise default credential chain
    let collector = iam_access_analyzer::IamAccessAnalyzerCollector::new(&aws_config).await
        .map_err(|e| anyhow::anyhow!("Failed to create IAM Access Analyzer collector: {}", e))?;

    // List analyzers
    let analyzers = collector.list_analyzers().await
        .map_err(|e| anyhow::anyhow!("Failed to list IAM Access Analyzers: {}", e))?;

    if analyzers.is_empty() {
        info!("No IAM Access Analyzers found for integration {}", config.id);
        return Ok(());
    }

    // Collect findings for all analyzers
    let metrics = collector
        .collect_findings_batch(&analyzers)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect IAM Access Analyzer findings: {}", e))?;

    info!("Collected {} IAM Access Analyzer metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<iam_access_analyzer::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = iam_access_analyzer::iam_access_analyzer_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_iam_access_analyzer_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store IAM Access Analyzer metrics: {}", e))?;

    info!("Stored {} IAM Access Analyzer metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store IAM Access Analyzer metrics in ClickHouse (compatible with metrics API format)
async fn store_iam_access_analyzer_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[iam_access_analyzer::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store RDS metrics in ClickHouse (compatible with metrics API format)
async fn store_rds_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[rds::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store EC2 metrics in ClickHouse (compatible with metrics API format)
async fn store_ec2_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[ec2::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store Lambda metrics in ClickHouse (compatible with metrics API format)
async fn store_lambda_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[lambda::ReiverMetric],
) -> Result<()> {
    if metrics.is_empty() {
        return Ok(());
    }

    use clickhouse::Row;
    use serde::Serialize;

    #[derive(Row, Serialize)]
    struct MetricInsert {
        id: String,
        project_id: String,
        name: String,
        value: f64,
        r#type: String,
        tags: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        created_at: DateTime<Utc>,
    }

    let mut inserter = clickhouse
        .as_ref()
        .inserter::<MetricInsert>("metrics")
        .with_period(Some(StdDuration::from_millis(100)))
        .with_max_rows(500_000);

    let now = Utc::now();
    let project_id_str = project_id.to_string();

    for metric in metrics {
        let tags_str = serde_json::to_string(&metric.tags)
            .unwrap_or_else(|_| "[]".to_string());

        let metric_row = MetricInsert {
            id: Uuid::new_v4().to_string(),
            project_id: project_id_str.clone(),
            name: metric.name.clone(),
            value: metric.value,
            r#type: metric.r#type.clone(),
            tags: tags_str,
            timestamp: metric.timestamp,
            created_at: now,
        };

        inserter.write(&metric_row).await
            .map_err(|e| anyhow::anyhow!("Failed to write metric to inserter: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Check if collection should proceed based on interval
fn should_collect_now(
    config_id: Uuid,
    collection_interval_seconds: i32,
    last_times: &HashMap<Uuid, DateTime<Utc>>,
) -> bool {
    let now = Utc::now();
    match last_times.get(&config_id) {
        Some(last_time) => {
            let elapsed = now.signed_duration_since(*last_time);
            elapsed.num_seconds() >= collection_interval_seconds as i64
        }
        None => true, // Never collected, should collect now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_should_collect_now_never_collected() {
        let config_id = Uuid::new_v4();
        let last_times: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        assert!(should_collect_now(config_id, 60, &last_times));
    }

    #[test]
    fn test_should_collect_now_interval_elapsed() {
        let config_id = Uuid::new_v4();
        let mut last_times: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        last_times.insert(config_id, Utc::now() - Duration::minutes(2));
        assert!(should_collect_now(config_id, 60, &last_times));
    }

    #[test]
    fn test_should_collect_now_interval_not_elapsed() {
        let config_id = Uuid::new_v4();
        let mut last_times: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        last_times.insert(config_id, Utc::now() - Duration::seconds(30));
        assert!(!should_collect_now(config_id, 60, &last_times));
    }

    #[test]
    fn test_should_collect_now_exactly_at_interval() {
        let config_id = Uuid::new_v4();
        let mut last_times: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        last_times.insert(config_id, Utc::now() - Duration::seconds(60));
        assert!(should_collect_now(config_id, 60, &last_times));
    }

    #[test]
    fn test_cleanup_stale_entries_logic() {
        let active_config = Uuid::new_v4();
        let stale_config = Uuid::new_v4();
        
        let mut last_times: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        last_times.insert(active_config, Utc::now());
        last_times.insert(stale_config, Utc::now());
        
        let active_ids: std::collections::HashSet<Uuid> = vec![active_config].into_iter().collect();
        last_times.retain(|id, _| active_ids.contains(id));
        
        assert!(last_times.contains_key(&active_config));
        assert!(!last_times.contains_key(&stale_config));
    }

    #[test]
    fn test_collection_interval_range() {
        let valid_intervals = vec![30, 60, 120, 300, 600];
        for interval in valid_intervals {
            assert!(interval >= 30);
            assert!(interval <= 3600);
        }
    }
}
