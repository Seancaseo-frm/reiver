use clickhouse::Client;
use std::sync::Arc;

pub type ClickHousePool = Arc<Client>;

pub fn create_clickhouse_pool(url: &str) -> anyhow::Result<ClickHousePool> {
    let client = Client::default().with_url(url).with_database("reiver");

    Ok(Arc::new(client))
}

/// Connect to ClickHouse using klickhouse (native protocol) for running refinery migrations.
/// Connects to the `default` database (not `reiver`) so that the first migration can
/// CREATE DATABASE on a fresh node. All migration SQL uses fully-qualified table names.
pub async fn connect_for_migrations(url: &str) -> anyhow::Result<klickhouse::Client> {
    use klickhouse::{Client as KlickhouseClient, ClientOptions};

    // Parse URL to extract host and port for native protocol
    let parsed_url = url::Url::parse(url)
        .map_err(|e| anyhow::anyhow!("Failed to parse ClickHouse URL: {}", e))?;

    let host = parsed_url.host_str().unwrap_or("localhost");
    // klickhouse uses native protocol, not HTTP (port 8123).
    // The ClickHouse operator exposes tcp_port on 9001 and a protocol-stack
    // native port on 9000. Use CLICKHOUSE_NATIVE_PORT env var if set,
    // otherwise default to 9000 for non-operator setups.
    let url_port = parsed_url.port().unwrap_or(8123);
    let port = if url_port == 8123 {
        std::env::var("CLICKHOUSE_NATIVE_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(9000u16)
    } else {
        url_port
    };
    let username = parsed_url.username();
    let password = parsed_url.password().unwrap_or("");

    let options = ClientOptions {
        username: if username.is_empty() {
            "default".to_string()
        } else {
            username.to_string()
        },
        password: password.to_string(),
        default_database: "default".to_string(),
        ..Default::default()
    };

    let addr = format!("{}:{}", host, port);
    tracing::info!(
        "Connecting to ClickHouse for migrations at {} (native protocol)",
        addr
    );

    let mut client = None;
    let mut last_error = None;
    for attempt in 1..=5 {
        match KlickhouseClient::connect(&addr, options.clone()).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(e) => {
                tracing::warn!(
                    "ClickHouse migration connection attempt {}/5 failed: {}",
                    attempt,
                    e
                );
                last_error = Some(e);
                if attempt < 5 {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    client.ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to connect to ClickHouse for migrations after 5 attempts: {:?}",
            last_error
        )
    })
}

/// Initialize Watch-specific ClickHouse Kafka Engine tables and Materialized Views.
/// Creates Kafka Engine table for exceptions ingestion.
///
/// These tables are NOT managed by ClickHouse migrations (in `website/clickhouse_migrations/`)
/// because the `ENGINE = Kafka(...)` clause requires runtime configuration values (broker
/// addresses, topic names) that come from environment variables and differ per environment.
pub async fn init_watch_kafka_engine(
    client: &ClickHousePool,
    kafka_hosts: &str,
    kafka_exceptions_topic: &str,
) -> anyhow::Result<()> {
    tracing::info!(
        "Initializing Watch ClickHouse Kafka Engine tables with Kafka hosts: {}",
        kafka_hosts
    );

    // Create Kafka Engine table for exceptions
    tracing::info!("Creating Kafka Engine table for exceptions...");
    let exceptions_kafka_table = format!(
        "CREATE TABLE IF NOT EXISTS reiver.exceptions_kafka (
            value String
        ) ENGINE = Kafka('{}', '{}', 'clickhouse-exceptions-consumer-group', 'JSONAsString')
        SETTINGS
            kafka_poll_max_batch_size = 100000,
            kafka_flush_interval_ms = 7500,
            kafka_max_block_size = 1000000,
            kafka_skip_broken_messages = 1",
        kafka_hosts, kafka_exceptions_topic
    );

    client
        .query(&exceptions_kafka_table)
        .execute()
        .await
        .map_err(|e| {
            anyhow::anyhow!("Failed to create Kafka Engine table for exceptions: {}", e)
        })?;

    // Create Materialized View for exceptions
    tracing::info!("Creating Materialized View for exceptions...");
    let exceptions_mv = r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.exceptions_mv TO reiver.exceptions_local AS
        SELECT
            JSONExtractString(value, 'id') AS id,
            JSONExtractString(value, 'project_id') AS project_id,
            JSONExtractString(value, 'fingerprint') AS fingerprint,
            JSONExtractString(value, 'level') AS level,
            JSONExtractString(value, 'message') AS message,
            coalesce(nullIf(JSONExtractString(value, 'exception_type'), ''), '') AS exception_type,
            coalesce(nullIf(JSONExtractString(value, 'exception_value'), ''), '') AS exception_value,
            coalesce(JSONExtractString(value, 'stacktrace'), '') AS stacktrace,
            coalesce(JSONExtractString(value, 'context'), '') AS context,
            coalesce(JSONExtractString(value, 'tags'), '') AS tags,
            coalesce(JSONExtractString(value, 'user_data'), '') AS user_data,
            coalesce(nullIf(JSONExtractString(value, 'service_name'), ''), '') AS service_name,
            coalesce(nullIf(JSONExtractString(value, 'trace_id'), ''), '') AS trace_id,
            coalesce(nullIf(JSONExtractString(value, 'span_id'), ''), '') AS span_id,
            coalesce(nullIf(JSONExtractString(value, 'service_version'), ''), '') AS service_version,
            coalesce(nullIf(JSONExtractString(value, 'environment'), ''), '') AS environment,
            coalesce(nullIf(JSONExtractString(value, 'repository_url'), ''), '') AS repository_url,
            coalesce(nullIf(JSONExtractString(value, 'status'), ''), 'unresolved') AS status,
            parseDateTime64BestEffort(JSONExtractString(value, 'timestamp'), 9) AS timestamp,
            now64() AS created_at
        FROM reiver.exceptions_kafka
        WHERE JSONHas(value, 'id')
    "#;

    client
        .query(exceptions_mv)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Materialized View for exceptions: {}", e))?;
    tracing::info!("Kafka Engine setup for exceptions completed");

    tracing::info!("Watch ClickHouse Kafka Engine initialization completed successfully");
    Ok(())
}

/// Initialize Flow-specific ClickHouse Kafka Engine tables and Materialized Views.
/// Creates Kafka Engine tables for LLM chunks ingestion.
///
/// These tables are NOT managed by ClickHouse migrations (in `website/clickhouse_migrations/`)
/// because the `ENGINE = Kafka(...)` clause requires runtime configuration values (broker
/// addresses, topic names) that come from environment variables and differ per environment.
pub async fn init_flow_kafka_engine(
    client: &ClickHousePool,
    kafka_hosts: &str,
    kafka_llm_chunks_topic: &str,
) -> anyhow::Result<()> {
    tracing::info!(
        "Initializing Flow ClickHouse Kafka Engine tables with Kafka hosts: {}",
        kafka_hosts
    );

    // LLM Chunks Kafka Engine
    tracing::info!("Creating Kafka Engine table for LLM chunks...");
    let llm_chunks_kafka_table = format!(
        "CREATE TABLE IF NOT EXISTS reiver.llm_chunks_kafka (
            value String
        ) ENGINE = Kafka('{}', '{}', 'clickhouse-llm-chunks-consumer', 'JSONAsString')
        SETTINGS
            kafka_poll_max_batch_size = 100000,
            kafka_flush_interval_ms = 5000,
            kafka_max_block_size = 1000000,
            kafka_skip_broken_messages = 1",
        kafka_hosts, kafka_llm_chunks_topic
    );

    client
        .query(&llm_chunks_kafka_table)
        .execute()
        .await
        .map_err(|e| {
            anyhow::anyhow!("Failed to create Kafka Engine table for LLM chunks: {}", e)
        })?;

    // Materialized View: llm_chunks_kafka -> llm_chunks
    tracing::info!("Creating Materialized View for LLM chunks...");
    let llm_chunks_mv = r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.llm_chunks_mv TO reiver.llm_chunks_local AS
        SELECT
            JSONExtractString(value, 'project_id') AS project_id,
            JSONExtractString(value, 'request_id') AS request_id,
            JSONExtractUInt(value, 'chunk_index') AS chunk_index,
            JSONExtractString(value, 'content') AS content,
            JSONExtractString(value, 'model') AS model,
            JSONExtractString(value, 'provider') AS provider,
            parseDateTime64BestEffort(JSONExtractString(value, 'timestamp'), 3) AS timestamp,
            toUInt8(JSONExtractBool(value, 'is_final')) AS is_final,
            coalesce(nullIf(JSONExtractString(value, 'finish_reason'), ''), '') AS finish_reason,
            coalesce(JSONExtractUInt(value, 'input_tokens'), 0) AS input_tokens,
            coalesce(JSONExtractUInt(value, 'output_tokens'), 0) AS output_tokens
        FROM reiver.llm_chunks_kafka
        WHERE JSONHas(value, 'request_id')
    "#;

    client
        .query(llm_chunks_mv)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Materialized View for LLM chunks: {}", e))?;
    tracing::info!("Kafka Engine setup for LLM chunks completed");

    tracing::info!("Flow ClickHouse Kafka Engine initialization completed successfully");
    Ok(())
}

/// Required ClickHouse tables that Flow (and other services) expect to exist.
/// These are created by the Website service's ClickHouse migrations.
pub const REQUIRED_CLICKHOUSE_TABLES: &[&str] = &["llm_requests", "provider_latency_samples"];

/// Verify that the given tables exist in the `reiver` database.
/// Fails with a clear error if any table is missing (e.g. ClickHouse migrations not run).
pub async fn validate_clickhouse_tables(
    client: &ClickHousePool,
    tables: &[&str],
) -> anyhow::Result<()> {
    use clickhouse::Row;
    use serde::Deserialize;

    #[derive(Row, Deserialize)]
    struct TableCountRow {
        n: u64,
    }

    for table in tables {
        let query = format!(
            "SELECT count() AS n FROM system.tables WHERE database = 'reiver' AND name = '{}'",
            table
        );
        let row: TableCountRow = client
            .query(&query)
            .fetch_one::<TableCountRow>()
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse error checking table '{}': {}", table, e))?;
        if row.n == 0 {
            anyhow::bail!(
                "Required ClickHouse table 'reiver.{}' does not exist. \
                 The website service must run ClickHouse migrations before flow starts. \
                 Start the website service once to apply migrations, or run them manually.",
                table
            );
        }
    }
    Ok(())
}
