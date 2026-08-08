//! OCI integrations background worker
//!
//! Polls OCI services (Compute, etc.) for metrics and stores them in ClickHouse

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
use reiver_integrations_server_oci::{
    compute,
    container_instances,
    database,
    functions,
    load_balancer,
    object_storage,
    oke,
    config::OciConfig,
};

use compute::ReiverMetric as OciComputeReiverMetric;
use container_instances::ReiverMetric as OciContainerInstanceReiverMetric;
use database::ReiverMetric as OciDatabaseReiverMetric;
use functions::ReiverMetric as OciFunctionReiverMetric;
use load_balancer::ReiverMetric as OciLoadBalancerReiverMetric;
use object_storage::ReiverMetric as OciObjectStorageReiverMetric;
use oke::ReiverMetric as OciOkeReiverMetric;

/// Database row structure for OCI integration configs
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct OciIntegrationConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    tenancy_ocid: String,
    user_ocid: String,
    fingerprint: String,
    private_key: String, // Private key (should be encrypted in production)
    region: String,
    passphrase: Option<String>, // Passphrase (should be encrypted in production)
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Decrypted OCI config for use by collectors
struct DecryptedOciConfig {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    tenancy_ocid: String,
    user_ocid: String,
    fingerprint: String,
    private_key: String, // Decrypted
    region: String,
    passphrase: Option<String>, // Decrypted
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Start OCI integrations worker
/// Polls OCI integration configs from database and collects metrics
pub async fn start_oci_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    encryptor: Arc<RotatingSecretEncryptor>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting OCI integrations worker...");

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(StdDuration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = collect_all_oci_metrics(&db_pool, &clickhouse_pool, &encryptor).await {
                        error!("OCI worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("OCI worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("OCI worker stopped");
    });

    Ok(handle)
}

/// Collect metrics from all enabled OCI integrations
async fn collect_all_oci_metrics(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    encryptor: &RotatingSecretEncryptor,
) -> Result<()> {
    // Fetch all enabled OCI integration configs
    let configs: Vec<OciIntegrationConfigRow> = sqlx::query_as::<_, OciIntegrationConfigRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            tenancy_ocid,
            user_ocid,
            fingerprint,
            private_key,
            region,
            passphrase,
            enabled,
            collection_interval_seconds,
            config_jsonb
        FROM oci_integration_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to fetch OCI integration configs: {}", e))?;

    info!("Found {} enabled OCI integrations", configs.len());

    // Decrypt secrets and create decrypted configs
    let decrypted_configs: Vec<DecryptedOciConfig> = configs.into_iter().filter_map(|row| {
        let private_key = match encryptor.decrypt(&row.private_key) {
            Ok(k) => k,
            Err(e) => {
                error!("Failed to decrypt private key for OCI integration {}: {}", row.id, e);
                return None;
            }
        };
        let passphrase = row.passphrase.and_then(|encrypted| {
            encryptor.decrypt(&encrypted).ok()
        });
        
        Some(DecryptedOciConfig {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            tenancy_ocid: row.tenancy_ocid,
            user_ocid: row.user_ocid,
            fingerprint: row.fingerprint,
            private_key,
            region: row.region,
            passphrase,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
        })
    }).collect();

    for config in decrypted_configs {
        match config.integration_type.as_str() {
            "compute" => {
                if let Err(e) = collect_compute_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Compute metrics for integration {}: {}", config.id, e);
                }
            }
            "functions" => {
                if let Err(e) = collect_functions_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Functions metrics for integration {}: {}", config.id, e);
                }
            }
            "object_storage" | "objectstorage" => {
                if let Err(e) = collect_object_storage_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Object Storage metrics for integration {}: {}", config.id, e);
                }
            }
            "database" | "autonomous_database" | "autonomousdatabase" => {
                if let Err(e) = collect_database_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Database metrics for integration {}: {}", config.id, e);
                }
            }
            "container_instances" | "containerinstances" => {
                if let Err(e) = collect_container_instances_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Container Instances metrics for integration {}: {}", config.id, e);
                }
            }
            "oke" | "oke_cluster" | "kubernetes" | "kubernetes_engine" => {
                if let Err(e) = collect_oke_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI OKE metrics for integration {}: {}", config.id, e);
                }
            }
            "load_balancer" | "loadbalancer" | "lbaas" => {
                if let Err(e) = collect_load_balancer_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect OCI Load Balancer metrics for integration {}: {}", config.id, e);
                }
            }
            _ => {
                warn!("Unsupported OCI integration type: {}", config.integration_type);
            }
        }
    }

    Ok(())
}

/// Collect OCI Compute metrics for a specific integration config
async fn collect_compute_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Compute metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Compute collector
    let collector = compute::OciComputeCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Compute integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all instances in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Compute metrics: {}", e))?;

    info!("Collected {} OCI Compute metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciComputeReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = compute::oci_compute_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_compute_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Compute metrics: {}", e))?;

    info!("Stored {} OCI Compute metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect OCI Functions metrics for a specific integration config
async fn collect_functions_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Functions metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Functions collector
    let collector = functions::OciFunctionCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Functions integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all functions in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Functions metrics: {}", e))?;

    info!("Collected {} OCI Functions metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciFunctionReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = functions::oci_function_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_functions_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Functions metrics: {}", e))?;

    info!("Stored {} OCI Functions metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI Functions metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_functions_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciFunctionReiverMetric],
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

/// Collect OCI Object Storage metrics for a specific integration config
async fn collect_object_storage_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Object Storage metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Object Storage collector
    let collector = object_storage::OciObjectStorageCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Object Storage integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all buckets in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Object Storage metrics: {}", e))?;

    info!("Collected {} OCI Object Storage metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciObjectStorageReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = object_storage::oci_object_storage_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_object_storage_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Object Storage metrics: {}", e))?;

    info!("Stored {} OCI Object Storage metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI Object Storage metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_object_storage_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciObjectStorageReiverMetric],
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

/// Collect OCI Database metrics for a specific integration config
async fn collect_database_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Database metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Database collector
    let collector = database::OciDatabaseCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Database integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all databases in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Database metrics: {}", e))?;

    info!("Collected {} OCI Database metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciDatabaseReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = database::oci_database_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_database_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Database metrics: {}", e))?;

    info!("Stored {} OCI Database metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI Database metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_database_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciDatabaseReiverMetric],
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

/// Collect OCI Container Instances metrics for a specific integration config
async fn collect_container_instances_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Container Instances metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Container Instances collector
    let collector = container_instances::OciContainerInstanceCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Container Instances integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all container instances in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Container Instances metrics: {}", e))?;

    info!("Collected {} OCI Container Instances metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciContainerInstanceReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = container_instances::oci_container_instance_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_container_instances_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Container Instances metrics: {}", e))?;

    info!("Stored {} OCI Container Instances metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI Container Instances metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_container_instances_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciContainerInstanceReiverMetric],
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

/// Collect OCI OKE metrics for a specific integration config
async fn collect_oke_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI OKE metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI OKE collector
    let collector = oke::OciOkeCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI OKE integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all OKE clusters in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI OKE metrics: {}", e))?;

    info!("Collected {} OCI OKE cluster metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciOkeReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = oke::oci_oke_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_oke_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI OKE metrics: {}", e))?;

    info!("Stored {} OCI OKE metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI OKE metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_oke_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciOkeReiverMetric],
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

/// Collect OCI Load Balancer metrics for a specific integration config
async fn collect_load_balancer_metrics(
    config: &DecryptedOciConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting OCI Load Balancer metrics for integration: {}", config.id);

    // Create OCI config from database config
    let oci_config = OciConfig {
        tenancy_ocid: config.tenancy_ocid.clone(),
        user_ocid: config.user_ocid.clone(),
        fingerprint: config.fingerprint.clone(),
        private_key: config.private_key.clone(),
        region: config.region.clone(),
        passphrase: config.passphrase.clone(),
    };

    // Create OCI Load Balancer collector
    let collector = load_balancer::OciLoadBalancerCollector::new(oci_config);

    // Get compartment ID from config_jsonb (required for OCI API)
    let compartment_id = config.config_jsonb
        .get("compartment_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            warn!("OCI Load Balancer integration {} missing compartment_id in config_jsonb, using tenancy_ocid", config.id);
            config.tenancy_ocid.clone()
        });

    // Collect metrics for all load balancers in the compartment
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_all_metrics(&compartment_id, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect OCI Load Balancer metrics: {}", e))?;

    info!("Collected {} OCI Load Balancer metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<OciLoadBalancerReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = load_balancer::oci_load_balancer_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_oci_load_balancer_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store OCI Load Balancer metrics: {}", e))?;

    info!("Stored {} OCI Load Balancer metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store OCI Load Balancer metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_load_balancer_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciLoadBalancerReiverMetric],
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

/// Store OCI Compute metrics in ClickHouse (compatible with metrics API format)
async fn store_oci_compute_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[OciComputeReiverMetric],
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
