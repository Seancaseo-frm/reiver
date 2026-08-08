//! Azure integrations background worker
//!
//! Polls Azure services (VMs, etc.) for metrics and stores them in ClickHouse

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
use reiver_integrations_server_azure::{
    compute,
    functions,
    storage,
    sql_database,
    cosmosdb,
    redis_cache,
    container_instances,
    aks,
    app_services,
    service_bus,
    event_hub,
    api_management,
    application_gateway,
    synapse_analytics,
    config::AzureConfig,
};

/// Database row structure for Azure integration configs
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct AzureIntegrationConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    subscription_id: String,
    tenant_id: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>, // Encrypted client secret
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Start Azure integrations worker
/// Polls Azure integration configs from database and collects metrics
pub async fn start_azure_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    encryptor: Arc<RotatingSecretEncryptor>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting Azure integrations worker...");

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(StdDuration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = collect_all_azure_metrics(&db_pool, &clickhouse_pool, &encryptor).await {
                        error!("Azure worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Azure worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Azure worker stopped");
    });

    Ok(handle)
}

/// Decrypted Azure config for use by collectors
struct DecryptedAzureConfig {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    subscription_id: String,
    tenant_id: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>, // Decrypted secret
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Collect metrics from all enabled Azure integrations
async fn collect_all_azure_metrics(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    encryptor: &RotatingSecretEncryptor,
) -> Result<()> {
    // Fetch all enabled Azure integration configs
    let configs: Vec<AzureIntegrationConfigRow> = sqlx::query_as::<_, AzureIntegrationConfigRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            subscription_id,
            tenant_id,
            client_id,
            client_secret,
            enabled,
            collection_interval_seconds,
            config_jsonb
        FROM azure_integration_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to fetch Azure integration configs: {}", e))?;

    info!("Found {} enabled Azure integrations", configs.len());
    
    // Decrypt secrets for each config
    let configs: Vec<DecryptedAzureConfig> = configs.into_iter().filter_map(|row| {
        let client_secret = row.client_secret.and_then(|encrypted| {
            encryptor.decrypt(&encrypted).ok()
        });
        
        Some(DecryptedAzureConfig {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            subscription_id: row.subscription_id,
            tenant_id: row.tenant_id,
            client_id: row.client_id,
            client_secret,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
        })
    }).collect();

    for config in configs {
        match config.integration_type.as_str() {
            "vm" => {
                if let Err(e) = collect_vm_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure VM metrics for integration {}: {}", config.id, e);
                }
            }
            "functions" => {
                if let Err(e) = collect_functions_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Functions metrics for integration {}: {}", config.id, e);
                }
            }
            "blob_storage" => {
                if let Err(e) = collect_blob_storage_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Blob Storage metrics for integration {}: {}", config.id, e);
                }
            }
            "sql_database" => {
                if let Err(e) = collect_sql_database_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure SQL Database metrics for integration {}: {}", config.id, e);
                }
            }
            "cosmosdb" => {
                if let Err(e) = collect_cosmosdb_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure CosmosDB metrics for integration {}: {}", config.id, e);
                }
            }
            "redis_cache" => {
                if let Err(e) = collect_redis_cache_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Redis Cache metrics for integration {}: {}", config.id, e);
                }
            }
            "container_instances" => {
                if let Err(e) = collect_container_instances_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Container Instances metrics for integration {}: {}", config.id, e);
                }
            }
            "aks" => {
                if let Err(e) = collect_aks_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure AKS metrics for integration {}: {}", config.id, e);
                }
            }
            "app_services" => {
                if let Err(e) = collect_app_services_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure App Services metrics for integration {}: {}", config.id, e);
                }
            }
            "service_bus" => {
                if let Err(e) = collect_service_bus_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Service Bus metrics for integration {}: {}", config.id, e);
                }
            }
            "event_hub" => {
                if let Err(e) = collect_event_hub_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Event Hub metrics for integration {}: {}", config.id, e);
                }
            }
            "api_management" => {
                if let Err(e) = collect_api_management_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure API Management metrics for integration {}: {}", config.id, e);
                }
            }
            "application_gateway" => {
                if let Err(e) = collect_application_gateway_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Application Gateway metrics for integration {}: {}", config.id, e);
                }
            }
            "synapse_analytics" | "synapse" => {
                if let Err(e) = collect_synapse_analytics_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Azure Synapse Analytics metrics for integration {}: {}", config.id, e);
                }
            }
            _ => {
                warn!("Unsupported Azure integration type: {}", config.integration_type);
            }
        }
    }

    Ok(())
}

/// Collect Azure VM metrics for a specific integration config
async fn collect_vm_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure VM metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure VM collector
    let collector = compute::AzureVmCollector::new(azure_config);

    // List VMs
    let vms = collector.list_vms().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure VMs: {}", e))?;

    if vms.is_empty() {
        info!("No Azure VMs found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all VMs
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&vms, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure VM metrics: {}", e))?;

    info!("Collected {} Azure VM metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<compute::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = compute::azure_vm_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_vm_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure VM metrics: {}", e))?;

    info!("Stored {} Azure VM metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect Azure Functions metrics for a specific integration config
async fn collect_functions_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Functions metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Functions collector
    let collector = functions::AzureFunctionsCollector::new(azure_config);

    // List Function Apps
    let function_apps = collector.list_function_apps().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Function Apps: {}", e))?;

    if function_apps.is_empty() {
        info!("No Azure Function Apps found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Function Apps
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&function_apps, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Functions metrics: {}", e))?;

    info!("Collected {} Azure Functions metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<functions::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = functions::azure_functions_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_functions_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Functions metrics: {}", e))?;

    info!("Stored {} Azure Functions metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Functions metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_functions_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[functions::ReiverMetric],
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

/// Collect Azure Blob Storage metrics for a specific integration config
async fn collect_blob_storage_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Blob Storage metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Blob Storage collector
    let collector = storage::AzureBlobStorageCollector::new(azure_config);

    // List Storage Accounts
    let storage_accounts = collector.list_storage_accounts().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Storage Accounts: {}", e))?;

    if storage_accounts.is_empty() {
        info!("No Azure Storage Accounts found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Storage Accounts
    let end_time = Utc::now();
    let start_time = end_time - Duration::hours(1); // Collect last hour of metrics (Blob Storage metrics are hourly)

    let metrics = collector
        .collect_metrics_batch(&storage_accounts, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Blob Storage metrics: {}", e))?;

    info!("Collected {} Azure Blob Storage metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<storage::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = storage::azure_blob_storage_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_blob_storage_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Blob Storage metrics: {}", e))?;

    info!("Stored {} Azure Blob Storage metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Blob Storage metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_blob_storage_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[storage::ReiverMetric],
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

/// Collect Azure SQL Database metrics for a specific integration config
async fn collect_sql_database_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure SQL Database metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure SQL Database collector
    let collector = sql_database::AzureSqlDatabaseCollector::new(azure_config);

    // List SQL Databases
    let databases = collector.list_databases().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure SQL Databases: {}", e))?;

    if databases.is_empty() {
        info!("No Azure SQL Databases found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all SQL Databases
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&databases, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure SQL Database metrics: {}", e))?;

    info!("Collected {} Azure SQL Database metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<sql_database::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = sql_database::azure_sql_database_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_sql_database_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure SQL Database metrics: {}", e))?;

    info!("Stored {} Azure SQL Database metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure SQL Database metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_sql_database_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[sql_database::ReiverMetric],
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

/// Collect Azure CosmosDB metrics for a specific integration config
async fn collect_cosmosdb_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure CosmosDB metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure CosmosDB collector
    let collector = cosmosdb::AzureCosmosDbCollector::new(azure_config);

    // List CosmosDB Accounts
    let accounts = collector.list_accounts().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure CosmosDB Accounts: {}", e))?;

    if accounts.is_empty() {
        info!("No Azure CosmosDB Accounts found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all CosmosDB Accounts
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&accounts, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure CosmosDB metrics: {}", e))?;

    info!("Collected {} Azure CosmosDB metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<cosmosdb::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = cosmosdb::azure_cosmosdb_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_cosmosdb_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure CosmosDB metrics: {}", e))?;

    info!("Stored {} Azure CosmosDB metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure CosmosDB metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_cosmosdb_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[cosmosdb::ReiverMetric],
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

/// Collect Azure Redis Cache metrics for a specific integration config
async fn collect_redis_cache_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Redis Cache metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Redis Cache collector
    let collector = redis_cache::AzureRedisCacheCollector::new(azure_config);

    // List Redis Cache instances
    let caches = collector.list_caches().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Redis Cache instances: {}", e))?;

    if caches.is_empty() {
        info!("No Azure Redis Cache instances found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Redis Cache instances
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&caches, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Redis Cache metrics: {}", e))?;

    info!("Collected {} Azure Redis Cache metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<redis_cache::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = redis_cache::azure_redis_cache_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_redis_cache_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Redis Cache metrics: {}", e))?;

    info!("Stored {} Azure Redis Cache metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Redis Cache metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_redis_cache_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[redis_cache::ReiverMetric],
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

/// Collect Azure Container Instances metrics for a specific integration config
async fn collect_container_instances_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Container Instances metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Container Instances collector
    let collector = container_instances::AzureContainerInstancesCollector::new(azure_config);

    // List Container Groups
    let container_groups = collector.list_container_groups().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Container Groups: {}", e))?;

    if container_groups.is_empty() {
        info!("No Azure Container Groups found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Container Groups
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&container_groups, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Container Instances metrics: {}", e))?;

    info!("Collected {} Azure Container Instances metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<container_instances::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = container_instances::azure_container_instances_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_container_instances_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Container Instances metrics: {}", e))?;

    info!("Stored {} Azure Container Instances metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Container Instances metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_container_instances_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[container_instances::ReiverMetric],
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

/// Collect Azure AKS metrics for a specific integration config
async fn collect_aks_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure AKS metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure AKS collector
    let collector = aks::AzureAksCollector::new(azure_config);

    // List AKS clusters
    let clusters = collector.list_clusters().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure AKS clusters: {}", e))?;

    if clusters.is_empty() {
        info!("No Azure AKS clusters found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all AKS clusters
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&clusters, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure AKS metrics: {}", e))?;

    info!("Collected {} Azure AKS metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<aks::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = aks::azure_aks_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_aks_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure AKS metrics: {}", e))?;

    info!("Stored {} Azure AKS metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure AKS metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_aks_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[aks::ReiverMetric],
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

/// Collect Azure App Services metrics for a specific integration config
async fn collect_app_services_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure App Services metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure App Services collector
    let collector = app_services::AzureAppServicesCollector::new(azure_config);

    // List App Services
    let app_services = collector.list_app_services().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure App Services: {}", e))?;

    if app_services.is_empty() {
        info!("No Azure App Services found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all App Services
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&app_services, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure App Services metrics: {}", e))?;

    info!("Collected {} Azure App Services metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<app_services::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = app_services::azure_app_services_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_app_services_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure App Services metrics: {}", e))?;

    info!("Stored {} Azure App Services metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure App Services metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_app_services_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[app_services::ReiverMetric],
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

/// Collect Azure Service Bus metrics for a specific integration config
async fn collect_service_bus_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Service Bus metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Service Bus collector
    let collector = service_bus::AzureServiceBusCollector::new(azure_config);

    // List Service Bus Namespaces
    let namespaces = collector.list_namespaces().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Service Bus Namespaces: {}", e))?;

    if namespaces.is_empty() {
        info!("No Azure Service Bus Namespaces found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Service Bus Namespaces
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&namespaces, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Service Bus metrics: {}", e))?;

    info!("Collected {} Azure Service Bus metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<service_bus::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = service_bus::azure_service_bus_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_service_bus_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Service Bus metrics: {}", e))?;

    info!("Stored {} Azure Service Bus metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Service Bus metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_service_bus_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[service_bus::ReiverMetric],
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

/// Collect Azure Event Hub metrics for a specific integration config
async fn collect_event_hub_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Event Hub metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Event Hub collector
    let collector = event_hub::AzureEventHubCollector::new(azure_config);

    // List Event Hub Namespaces
    let namespaces = collector.list_namespaces().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Event Hub Namespaces: {}", e))?;

    if namespaces.is_empty() {
        info!("No Azure Event Hub Namespaces found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Event Hub Namespaces
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&namespaces, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Event Hub metrics: {}", e))?;

    info!("Collected {} Azure Event Hub metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<event_hub::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = event_hub::azure_event_hub_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_event_hub_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Event Hub metrics: {}", e))?;

    info!("Stored {} Azure Event Hub metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Event Hub metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_event_hub_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[event_hub::ReiverMetric],
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

/// Collect Azure API Management metrics for a specific integration config
async fn collect_api_management_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure API Management metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure API Management collector
    let collector = api_management::AzureApiManagementCollector::new(azure_config);

    // List API Management Services
    let services = collector.list_services().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure API Management Services: {}", e))?;

    if services.is_empty() {
        info!("No Azure API Management Services found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all API Management Services
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&services, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure API Management metrics: {}", e))?;

    info!("Collected {} Azure API Management metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<api_management::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = api_management::azure_api_management_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_api_management_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure API Management metrics: {}", e))?;

    info!("Stored {} Azure API Management metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure API Management metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_api_management_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[api_management::ReiverMetric],
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

/// Collect Azure Application Gateway metrics for a specific integration config
async fn collect_application_gateway_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Application Gateway metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Azure Application Gateway collector
    let collector = application_gateway::AzureApplicationGatewayCollector::new(azure_config);

    // List Application Gateways
    let gateways = collector.list_gateways().await
        .map_err(|e| anyhow::anyhow!("Failed to list Azure Application Gateways: {}", e))?;

    if gateways.is_empty() {
        info!("No Azure Application Gateways found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all Application Gateways
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&gateways, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Azure Application Gateway metrics: {}", e))?;

    info!("Collected {} Azure Application Gateway metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<application_gateway::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = application_gateway::azure_application_gateway_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_azure_application_gateway_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Azure Application Gateway metrics: {}", e))?;

    info!("Stored {} Azure Application Gateway metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Collect Azure Synapse Analytics metrics for a specific integration config
async fn collect_synapse_analytics_metrics(
    config: &DecryptedAzureConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Azure Synapse Analytics metrics for integration: {}", config.id);

    // Create Azure config from database config
    let azure_config = AzureConfig {
        subscription_id: config.subscription_id.clone(),
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        client_secret: config.client_secret.clone(),
    };

    // Create Synapse Analytics collector
    let collector = synapse_analytics::AzureSynapseAnalyticsCollector::new(azure_config);

    // List Synapse workspaces
    let workspaces = collector.list_workspaces().await
        .map_err(|e| anyhow::anyhow!("Failed to list Synapse workspaces: {}", e))?;

    if workspaces.is_empty() {
        info!("No Synapse workspaces found for integration {}", config.id);
        return Ok(());
    }

    // Collect metrics for all workspaces
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&workspaces, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Synapse Analytics metrics: {}", e))?;

    info!("Collected {} Synapse Analytics metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<synapse_analytics::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = synapse_analytics::azure_synapse_analytics_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_synapse_analytics_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Synapse Analytics metrics: {}", e))?;

    info!("Stored {} Synapse Analytics metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Azure Application Gateway metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_application_gateway_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[application_gateway::ReiverMetric],
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

/// Store Azure Synapse Analytics metrics in ClickHouse (compatible with metrics API format)
async fn store_synapse_analytics_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[synapse_analytics::ReiverMetric],
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
            .map_err(|e| anyhow::anyhow!("Failed to write Synapse Analytics metric to ClickHouse: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit Synapse Analytics metrics to ClickHouse: {}", e))?;

    Ok(())
}

/// Store Azure VM metrics in ClickHouse (compatible with metrics API format)
async fn store_azure_vm_metrics_in_clickhouse(
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
