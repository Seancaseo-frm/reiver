//! Snowflake integrations background worker
//!
//! Polls Snowflake accounts for metrics and stores them in ClickHouse

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
use reiver_integrations_server_snowflake::{
    collector::{SnowflakeCollector, snowflake_metrics_to_reiver_format},
    config::SnowflakeConfig,
};

/// Database row structure for Snowflake integration configs
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct SnowflakeIntegrationConfigRow {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    account: String,
    username: String,
    password: String, // Password (should be encrypted in production)
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Decrypted Snowflake config for use by collectors
struct DecryptedSnowflakeConfig {
    id: Uuid,
    project_id: Uuid,
    name: String,
    integration_type: String,
    account: String,
    username: String,
    password: String, // Decrypted password
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    enabled: bool,
    collection_interval_seconds: i32,
    config_jsonb: serde_json::Value,
}

/// Tracks last collection time per integration config ID
static LAST_COLLECTION_TIMES: once_cell::sync::Lazy<Mutex<HashMap<Uuid, DateTime<Utc>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Start Snowflake integrations worker
/// Polls Snowflake integration configs from database and collects metrics
pub async fn start_snowflake_worker(
    db_pool: Arc<DbPool>,
    clickhouse_pool: Arc<ClickHousePool>,
    encryptor: Arc<RotatingSecretEncryptor>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting Snowflake integrations worker...");

    let handle = tokio::spawn(async move {
        // Check every 10 seconds for configs that need collection
        let mut interval = tokio::time::interval(StdDuration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = collect_all_snowflake_metrics(&db_pool, &clickhouse_pool, &encryptor).await {
                        error!("Snowflake worker error: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Snowflake worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("Snowflake worker stopped");
    });

    Ok(handle)
}

/// Collect metrics from all enabled Snowflake integrations
async fn collect_all_snowflake_metrics(
    db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
    encryptor: &RotatingSecretEncryptor,
) -> Result<()> {
    // Fetch all enabled Snowflake integration configs
    // Note: This assumes a snowflake_integration_configs table exists
    // You may need to create a migration for this table
    let configs: Vec<SnowflakeIntegrationConfigRow> = sqlx::query_as::<_, SnowflakeIntegrationConfigRow>(
        r#"
        SELECT 
            id,
            project_id,
            name,
            integration_type,
            account,
            username,
            password,
            warehouse,
            database,
            schema,
            role,
            enabled,
            collection_interval_seconds,
            config_jsonb
        FROM snowflake_integration_configs
        WHERE enabled = true
        "#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to fetch Snowflake integration configs: {}", e))?;

    info!("Found {} enabled Snowflake integrations", configs.len());

    // Decrypt passwords and create decrypted configs
    let decrypted_configs: Vec<DecryptedSnowflakeConfig> = configs.into_iter().filter_map(|row| {
        // Decrypt password
        let password = match encryptor.decrypt(&row.password) {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to decrypt password for Snowflake integration {}: {}", row.id, e);
                return None;
            }
        };
        
        Some(DecryptedSnowflakeConfig {
            id: row.id,
            project_id: row.project_id,
            name: row.name,
            integration_type: row.integration_type,
            account: row.account,
            username: row.username,
            password,
            warehouse: row.warehouse,
            database: row.database,
            schema: row.schema,
            role: row.role,
            enabled: row.enabled,
            collection_interval_seconds: row.collection_interval_seconds,
            config_jsonb: row.config_jsonb,
        })
    }).collect();

    // Clean up stale entries from LAST_COLLECTION_TIMES for deleted configs
    {
        let config_ids: std::collections::HashSet<Uuid> = decrypted_configs.iter().map(|c| c.id).collect();
        let mut last_times = LAST_COLLECTION_TIMES.lock();
        last_times.retain(|id, _| config_ids.contains(id));
    }

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
            "snowflake" => {
                if let Err(e) = collect_snowflake_metrics(&config, db_pool, clickhouse_pool).await {
                    error!("Failed to collect Snowflake metrics for integration {}: {}", config.id, e);
                }
            }
            _ => {
                warn!("Unknown Snowflake integration type: {} for integration: {}", config.integration_type, config.id);
            }
        }
    }

    Ok(())
}

/// Collect Snowflake metrics for a specific integration config
async fn collect_snowflake_metrics(
    config: &DecryptedSnowflakeConfig,
    _db_pool: &DbPool,
    clickhouse_pool: &ClickHousePool,
) -> Result<()> {
    info!("Collecting Snowflake metrics for integration: {}", config.id);

    // Create Snowflake config from database config
    let snowflake_config = SnowflakeConfig {
        account: config.account.clone(),
        username: config.username.clone(),
        password: config.password.clone(),
        warehouse: config.warehouse.clone(),
        database: config.database.clone(),
        schema: config.schema.clone(),
        role: config.role.clone(),
    };

    // Create Snowflake collector
    let collector = SnowflakeCollector::new(snowflake_config);

    // List warehouses
    let warehouses = collector.list_warehouses().await
        .map_err(|e| anyhow::anyhow!("Failed to list Snowflake warehouses: {}", e))?;

    if warehouses.is_empty() {
        info!("No Snowflake warehouses found for integration {}", config.id);
        // Still collect account-level metrics
    }

    // Collect metrics for all warehouses
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(5); // Collect last 5 minutes of metrics

    let metrics = collector
        .collect_metrics_batch(&warehouses, start_time, end_time)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect Snowflake metrics: {}", e))?;

    info!("Collected {} Snowflake metric sets for integration {}", metrics.len(), config.id);

    // Convert to Reiver metric format and store in ClickHouse
    let project_id_str = config.project_id.to_string();
    let mut all_reiver_metrics: Vec<reiver_integrations_server_snowflake::collector::ReiverMetric> = Vec::new();

    for metric in &metrics {
        let reiver_metrics = snowflake_metrics_to_reiver_format(metric, &project_id_str);
        all_reiver_metrics.extend(reiver_metrics);
    }

    if all_reiver_metrics.is_empty() {
        info!("No metrics to store for integration {}", config.id);
        return Ok(());
    }

    // Store metrics in ClickHouse using the metrics API format
    store_snowflake_metrics_in_clickhouse(clickhouse_pool, config.project_id, &all_reiver_metrics).await
        .map_err(|e| anyhow::anyhow!("Failed to store Snowflake metrics: {}", e))?;

    info!("Stored {} Snowflake metrics for integration {}", all_reiver_metrics.len(), config.id);

    Ok(())
}

/// Store Snowflake metrics in ClickHouse (compatible with metrics API format)
async fn store_snowflake_metrics_in_clickhouse(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    metrics: &[reiver_integrations_server_snowflake::collector::ReiverMetric],
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
            .map_err(|e| anyhow::anyhow!("Failed to write Snowflake metric to ClickHouse: {}", e))?;
    }

    inserter.commit().await
        .map_err(|e| anyhow::anyhow!("Failed to commit Snowflake metrics to ClickHouse: {}", e))?;

    Ok(())
}
