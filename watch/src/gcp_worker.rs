//! GCP integrations background worker
//!
//! Polls GCP services (Compute Engine, etc.) for metrics and stores them in ClickHouse

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::clickhouse_db::ClickHousePool;
use crate::crypto::RotatingSecretEncryptor;
use crate::db::DbPool;
use std::collections::HashMap;
use parking_lot::Mutex;
use reiver_integrations_server_gcp::{
    compute,
    cloud_functions,
    cloud_storage,
    cloudsql,
    spanner,
    redis,
    cloud_run,
    gke,
    pubsub,
    load_balancing,
    monitoring,
    api_gateway,
    firestore,
    bigquery,
    config::GcpConfig,
};

/// Database row structure for GCP integration configs
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct GcpIntegrationConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    gcp_project_id: String,
    service_account_email: Option<String>,
    private_key: Option<String>, // Private key (should be encrypted in production)
    service_account_json: Option<String>, // Service account JSON (should be encrypted in production)
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Decrypted GCP config for use by collectors
struct DecryptedGcpConfig {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    gcp_project_id: String,
    service_account_email: Option<String>,
    private_key: Option<String>, // Decrypted
    service_account_json: Option<String>, // Decrypted
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Start GCP integrations worker
/// Polls GCP integration configs from database and collects metrics
/// Tracks last collection time per integration config ID
static LAST_COLLECTION_TIMES: once_cell::sync::Lazy<Mutex<HashMap<Uuid, DateTime<Utc>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

pub async fn start_gcp_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    encryptor: Arc<RotatingSecretEncryptor>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting GCP integrations worker...");

    let handle = tokio::spawn(async move {
        // Check every 10 seconds for configs that need collection
        let mut interval = tokio::time::interval(StdDuration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = collect_all_gcp_metrics(&db_pool, &clickhouse_pool, &encryptor).await {
                        error!("GCP worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("GCP worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("GCP worker stopped");
    });

    Ok(handle)
}

/// Collect metrics from all enabled GCP integrations
async fn collect_all_gcp_metrics(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    encryptor: &RotatingSecretEncryptor,
) -> Result<()> {
    // Fetch all enabled GCP integration configs
    let configs: Vec<GcpIntegrationConfigRow> = sqlx::query_as::<_, GcpIntegrationConfigRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            gcp_project_id,
            service_account_email,
            private_key,
            service_account_json,
            enabled,
            collection_interval_seconds,
            config_jsonb
        FROM gcp_integration_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to fetch GCP integration configs: {}", e))?;

    info!("Found {} enabled GCP integrations", configs.len());

    // Decrypt secrets and create decrypted configs
    let decrypted_configs: Vec<DecryptedGcpConfig> = configs.into_iter().map(|row| {
        let private_key = row.private_key.and_then(|encrypted| {
            encryptor.decrypt(&encrypted).ok()
        });
        let service_account_json = row.service_account_json.and_then(|encrypted| {
            encryptor.decrypt(&encrypted).ok()
        });
        
        DecryptedGcpConfig {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            gcp_project_id: row.gcp_project_id,
            service_account_email: row.service_account_email,
            private_key,
            service_account_json,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
        }
    }).collect();

    // Clean up stale entries from LAST_COLLECTION_TIMES for deleted configs
    {
        let config_ids: std::collections::HashSet<Uuid> = decrypted_configs.iter().map(|c| c.id).collect();
        let mut last_times = LAST_COLLECTION_TIMES.lock();
        last_times.retain(|id, _| config_ids.contains(id));
    }

    // Collect metrics for each integration
    for config in decrypted_configs {
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

        // Update last collection time before collecting
        {
            let mut last_times = LAST_COLLECTION_TIMES.lock();
            last_times.insert(config.id, now);
        }

        match config.integration_type.as_str() {
            "compute_engine" => {
                if let Err(e) = collect_compute_engine_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Compute Engine metrics for integration {}: {}", config.id, e);
                }
            }
            "cloud_functions" => {
                if let Err(e) = collect_cloud_functions_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Cloud Functions metrics for integration {}: {}", config.id, e);
                }
            }
            "cloud_storage" => {
                if let Err(e) = collect_cloud_storage_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Cloud Storage metrics for integration {}: {}", config.id, e);
                }
            }
            "cloudsql" => {
                if let Err(e) = collect_cloudsql_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP CloudSQL metrics for integration {}: {}", config.id, e);
                }
            }
            "cloud_spanner" | "spanner" => {
                if let Err(e) = collect_spanner_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Spanner metrics for integration {}: {}", config.id, e);
                }
            }
            "cloud_redis" | "redis" => {
                if let Err(e) = collect_redis_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Redis metrics for integration {}: {}", config.id, e);
                }
            }
            "cloud_run" | "run" => {
                if let Err(e) = collect_cloud_run_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Cloud Run metrics for integration {}: {}", config.id, e);
                }
            }
            "gke" | "kubernetes_engine" => {
                if let Err(e) = collect_gke_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP GKE metrics for integration {}: {}", config.id, e);
                }
            }
            "pubsub" | "pub_sub" => {
                if let Err(e) = collect_pubsub_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Pub/Sub metrics for integration {}: {}", config.id, e);
                }
            }
            "load_balancing" | "load_balancer" => {
                if let Err(e) = collect_load_balancing_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Load Balancing metrics for integration {}: {}", config.id, e);
                }
            }
            "monitoring" | "cloud_monitoring" => {
                if let Err(e) = collect_monitoring_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Cloud Monitoring metrics for integration {}: {}", config.id, e);
                }
            }
            "api_gateway" | "apigateway" => {
                if let Err(e) = collect_api_gateway_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP API Gateway metrics for integration {}: {}", config.id, e);
                }
            }
            "firestore" => {
                if let Err(e) = collect_firestore_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP Firestore metrics for integration {}: {}", config.id, e);
                }
            }
            "bigquery" => {
                if let Err(e) = collect_bigquery_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect GCP BigQuery metrics for integration {}: {}", config.id, e);
                }
            }
            _ => {
                warn!("Unsupported GCP integration type: {}", config.integration_type);
            }
        }
    }

    Ok(())
}

/// Collect GCP Compute Engine metrics for a specific integration config
async fn collect_compute_engine_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Compute Engine metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create GCE collector
    let collector = compute::GceCollector::new(gcp_config);

    // List Compute Engine instances
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list GCE instances: {}", e))?;

    if instances.is_empty() {
        info!("No GCE instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect GCE metrics: {}", e))?;

    info!("Collected {} GCE metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<compute::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = compute::gce_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_gce_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store GCE metrics: {}", e))?;

    info!("Stored {} GCE metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect GCP Cloud Functions metrics for a specific integration config
async fn collect_cloud_functions_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Functions metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Cloud Functions collector
    let collector = cloud_functions::CloudFunctionsCollector::new(gcp_config);

    // List Cloud Functions
    let functions = collector.list_functions().await
        .map_err(|e| anyhow::anyhow!("Failed to list Cloud Functions: {}", e))?;

    if functions.is_empty() {
        info!("No Cloud Functions found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all functions
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&functions, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Cloud Functions metrics: {}", e))?;

    info!("Collected {} Cloud Functions metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cloud_functions::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cloud_functions::cloud_functions_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_cloud_functions_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Cloud Functions metrics: {}", e))?;

    info!("Stored {} Cloud Functions metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Cloud Functions metrics in ClickHouse (compatible with metrics API format)
async fn store_cloud_functions_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cloud_functions::ReiverMetric],
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

/// Collect GCP Cloud Storage metrics for a specific integration config
async fn collect_cloud_storage_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Storage metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create GCS collector
    let collector = cloud_storage::GcsCollector::new(gcp_config);

    // List Cloud Storage buckets
    let buckets = collector.list_buckets().await
        .map_err(|e| anyhow::anyhow!("Failed to list GCS buckets: {}", e))?;

    if buckets.is_empty() {
        info!("No GCS buckets found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all buckets
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&buckets, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect GCS metrics: {}", e))?;

    info!("Collected {} GCS metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cloud_storage::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cloud_storage::gcs_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_cloud_storage_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store GCS metrics: {}", e))?;

    info!("Stored {} GCS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Cloud Storage metrics in ClickHouse (compatible with metrics API format)
async fn store_cloud_storage_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cloud_storage::ReiverMetric],
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

/// Collect GCP CloudSQL metrics for a specific integration config
async fn collect_cloudsql_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP CloudSQL metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create CloudSQL collector
    let collector = cloudsql::CloudSqlCollector::new(gcp_config);

    // List CloudSQL instances
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list CloudSQL instances: {}", e))?;

    if instances.is_empty() {
        info!("No CloudSQL instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect CloudSQL metrics: {}", e))?;

    info!("Collected {} CloudSQL metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cloudsql::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cloudsql::cloudsql_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_cloudsql_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store CloudSQL metrics: {}", e))?;

    info!("Stored {} CloudSQL metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store CloudSQL metrics in ClickHouse (compatible with metrics API format)
async fn store_cloudsql_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cloudsql::ReiverMetric],
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

/// Collect GCP Cloud Spanner metrics for a specific integration config
async fn collect_spanner_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Spanner metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Spanner collector
    let collector = spanner::SpannerCollector::new(gcp_config);

    // List Spanner instances
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list Spanner instances: {}", e))?;

    if instances.is_empty() {
        info!("No Spanner instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances (which will collect for all databases in each instance)
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Spanner metrics: {}", e))?;

    info!("Collected {} Spanner metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<spanner::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = spanner::spanner_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_spanner_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Spanner metrics: {}", e))?;

    info!("Stored {} Spanner metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Spanner metrics in ClickHouse (compatible with metrics API format)
async fn store_spanner_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[spanner::ReiverMetric],
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

/// Collect GCP Cloud Redis metrics for a specific integration config
async fn collect_redis_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Redis metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Redis collector
    let collector = redis::RedisCollector::new(gcp_config);

    // List Redis instances
    let instances = collector.list_instances().await
        .map_err(|e| anyhow::anyhow!("Failed to list Redis instances: {}", e))?;

    if instances.is_empty() {
        info!("No Redis instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&instances, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Redis metrics: {}", e))?;

    info!("Collected {} Redis metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<redis::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = redis::redis_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_redis_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Redis metrics: {}", e))?;

    info!("Stored {} Redis metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Redis metrics in ClickHouse (compatible with metrics API format)
async fn store_redis_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[redis::ReiverMetric],
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

/// Collect GCP Cloud Run metrics for a specific integration config
async fn collect_cloud_run_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Run metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Cloud Run collector
    let collector = cloud_run::CloudRunCollector::new(gcp_config);

    // List Cloud Run services
    let services = collector.list_services().await
        .map_err(|e| anyhow::anyhow!("Failed to list Cloud Run services: {}", e))?;

    if services.is_empty() {
        info!("No Cloud Run services found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all services (which will collect for all revisions in each service)
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&services, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Cloud Run metrics: {}", e))?;

    info!("Collected {} Cloud Run metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cloud_run::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cloud_run::cloud_run_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_cloud_run_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Cloud Run metrics: {}", e))?;

    info!("Stored {} Cloud Run metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Cloud Run metrics in ClickHouse (compatible with metrics API format)
async fn store_cloud_run_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cloud_run::ReiverMetric],
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

/// Collect GCP GKE metrics for a specific integration config
async fn collect_gke_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP GKE metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create GKE collector
    let collector = gke::GkeCollector::new(gcp_config);

    // List GKE clusters
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list GKE clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No GKE clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect GKE metrics: {}", e))?;

    info!("Collected {} GKE metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<gke::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = gke::gke_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_gke_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store GKE metrics: {}", e))?;

    info!("Stored {} GKE metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store GKE metrics in ClickHouse (compatible with metrics API format)
async fn store_gke_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[gke::ReiverMetric],
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

/// Collect GCP Pub/Sub metrics for a specific integration config
async fn collect_pubsub_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Pub/Sub metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Pub/Sub collector
    let collector = pubsub::PubSubCollector::new(gcp_config);

    // Collect metrics for all topics and subscriptions
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Pub/Sub metrics: {}", e))?;

    info!("Collected {} Pub/Sub metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<pubsub::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = pubsub::pubsub_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_pubsub_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Pub/Sub metrics: {}", e))?;

    info!("Stored {} Pub/Sub metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Pub/Sub metrics in ClickHouse (compatible with metrics API format)
async fn store_pubsub_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[pubsub::ReiverMetric],
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

/// Collect GCP Load Balancing metrics for a specific integration config
async fn collect_load_balancing_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Load Balancing metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Load Balancing collector
    let collector = load_balancing::LoadBalancingCollector::new(gcp_config);

    // Collect metrics for all load balancers
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Load Balancing metrics: {}", e))?;

    info!("Collected {} Load Balancing metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<load_balancing::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = load_balancing::load_balancing_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_load_balancing_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Load Balancing metrics: {}", e))?;

    info!("Stored {} Load Balancing metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Load Balancing metrics in ClickHouse (compatible with metrics API format)
async fn store_load_balancing_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[load_balancing::ReiverMetric],
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

/// Collect GCP Cloud Monitoring metrics for a specific integration config
async fn collect_monitoring_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Cloud Monitoring metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Cloud Monitoring collector
    let collector = monitoring::MonitoringCollector::new(gcp_config);

    // Extract filter(s) from config_jsonb
    let filters: Vec<String> = if let Some(filter_value) = config.config_jsonb.get("filter") {
        if let Some(filter_str) = filter_value.as_str() {
            vec![filter_str.to_string()]
        } else if let Some(filter_array) = filter_value.as_array() {
            filter_array
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            warn!("Invalid filter format in config_jsonb for integration {}", config.id);
            return Ok(());
        }
    } else if let Some(filters_value) = config.config_jsonb.get("filters") {
        if let Some(filters_array) = filters_value.as_array() {
            filters_array
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            warn!("Invalid filters format in config_jsonb for integration {}", config.id);
            return Ok(());
        }
    } else {
        warn!("No filter(s) found in config_jsonb for integration {}", config.id);
        return Ok(());
    };

    if filters.is_empty() {
        warn!("No filters to process for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all filters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&filters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Cloud Monitoring metrics: {}", e))?;

    info!("Collected {} Cloud Monitoring metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<monitoring::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = monitoring::monitoring_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_monitoring_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Cloud Monitoring metrics: {}", e))?;

    info!("Stored {} Cloud Monitoring metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Cloud Monitoring metrics in ClickHouse (compatible with metrics API format)
async fn store_monitoring_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[monitoring::ReiverMetric],
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

/// Collect GCP API Gateway metrics for a specific integration config
async fn collect_api_gateway_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP API Gateway metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create API Gateway collector
    let collector = api_gateway::ApiGatewayCollector::new(gcp_config);

    // Collect metrics for all API Gateways
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect API Gateway metrics: {}", e))?;

    info!("Collected {} API Gateway metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<api_gateway::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = api_gateway::api_gateway_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_api_gateway_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store API Gateway metrics: {}", e))?;

    info!("Stored {} API Gateway metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store API Gateway metrics in ClickHouse (compatible with metrics API format)
async fn store_api_gateway_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[api_gateway::ReiverMetric],
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

/// Collect GCP Firestore metrics for a specific integration config
async fn collect_firestore_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP Firestore metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create Firestore collector
    let collector = firestore::FirestoreCollector::new(gcp_config);

    // List Firestore databases
    let databases = collector.list_databases().await
        .map_err(|e| anyhow::anyhow!("Failed to list Firestore databases: {}", e))?;

    if databases.is_empty() {
        info!("No Firestore databases found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Firestore databases
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&databases, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Firestore metrics: {}", e))?;

    info!("Collected {} Firestore metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<firestore::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = firestore::firestore_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_firestore_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Firestore metrics: {}", e))?;

    info!("Stored {} Firestore metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect GCP BigQuery metrics for a specific integration config
async fn collect_bigquery_metrics(
    config: &DecryptedGcpConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting GCP BigQuery metrics for integration: {}", config.id);

    // Create GCP config from database config
    let gcp_config = GcpConfig {
        project_id: config.gcp_project_id.clone(),
        service_account_email: config.service_account_email.clone(),
        private_key: config.private_key.clone(),
        service_account_json: config.service_account_json.clone(),
    };

    // Create BigQuery collector
    let collector = bigquery::BigQueryCollector::new(gcp_config);

    // List BigQuery projects
    let projects = collector.list_projects().await
        .map_err(|e| anyhow::anyhow!("Failed to list BigQuery projects: {}", e))?;

    if projects.is_empty() {
        info!("No BigQuery projects found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all BigQuery projects
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&projects, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect BigQuery metrics: {}", e))?;

    info!("Collected {} BigQuery metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<bigquery::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = bigquery::bigquery_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_bigquery_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store BigQuery metrics: {}", e))?;

    info!("Stored {} BigQuery metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Firestore metrics in ClickHouse (compatible with metrics API format)
async fn store_firestore_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[firestore::ReiverMetric],
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

/// Store BigQuery metrics in ClickHouse (compatible with metrics API format)
async fn store_bigquery_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[bigquery::ReiverMetric],
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
            .map_err(|e| anyhow::anyhow!("Failed to write BigQuery metric to ClickHouse: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit BigQuery metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store GCE metrics in ClickHouse (compatible with metrics API format)
async fn store_gce_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[compute::ReiverMetric],
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
