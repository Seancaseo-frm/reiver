use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use regex::Regex;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub api: ApiConfig,
    pub intervals: IntervalsConfig,
    pub system_metrics: SystemMetricsConfig,
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub advanced: AdvancedConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConfig {
    pub url: String,
    pub api_key: String,
    pub timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IntervalsConfig {
    #[serde(default = "default_system_metrics_interval")]
    pub system_metrics: u64,
    #[serde(default = "default_database_metrics_interval")]
    pub database_metrics: u64,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat: u64,
}

fn default_system_metrics_interval() -> u64 { 60 }
fn default_database_metrics_interval() -> u64 { 60 }
fn default_heartbeat_interval() -> u64 { 300 }

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SystemMetricsConfig {
    pub enabled: bool,
    /// Hostname to tag system metrics with (defaults to system hostname if not set)
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub cpu: CpuConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub disk: DiskConfig,
    #[serde(default)]
    pub network: NetworkConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CpuConfig {
    pub enabled: bool,
    #[serde(default)]
    pub per_core: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MemoryConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiskConfig {
    pub enabled: bool,
    #[serde(default)]
    pub include_mounts: Vec<String>,
    #[serde(default)]
    pub exclude_mounts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct NetworkConfig {
    pub enabled: bool,
    #[serde(default)]
    pub interfaces: Vec<String>,
    #[serde(default = "default_true")]
    pub exclude_loopback: bool,
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub r#type: String, // "postgresql", "mysql", "mariadb", "redis", "mongodb", "clickhouse", "sqlserver", "oracle", "elasticsearch", "db2", "hana", "cockroachdb", "tidb", "yugabytedb", "singlestore", "cassandra", "couchdb", "couchbase"
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    
    // PostgreSQL specific
    #[serde(default)]
    pub pg_stat_statements: PostgresStatementsConfig,
    
    // MySQL specific
    #[serde(default)]
    pub performance_schema: PerformanceSchemaConfig,
    
    // Collection settings
    #[serde(default)]
    pub query_metrics: QueryMetricsConfig,
    #[serde(default)]
    pub explain_plans: ExplainPlansConfig,
    
    // Tags
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PostgresStatementsConfig {
    pub enabled: bool,
    #[serde(default = "default_statements_limit")]
    pub limit: i32,
}

fn default_statements_limit() -> i32 { 10000 }

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PerformanceSchemaConfig {
    pub enabled: bool,
    #[serde(default = "default_statements_limit")]
    pub limit: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct QueryMetricsConfig {
    pub enabled: bool,
    #[serde(default = "default_query_metrics_interval")]
    pub collection_interval: u64,
}

fn default_query_metrics_interval() -> u64 { 60 }

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ExplainPlansConfig {
    pub enabled: bool,
    #[serde(default = "default_slow_query_threshold")]
    pub slow_query_threshold_ms: f64,
    #[serde(default = "default_explain_interval")]
    pub collection_interval: u64,
}

fn default_slow_query_threshold() -> f64 { 1000.0 }
fn default_explain_interval() -> u64 { 60 }

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    pub file: Option<String>,
    #[serde(default = "default_true")]
    pub stdout: bool,
}

fn default_log_level() -> String { "info".to_string() }

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AdvancedConfig {
    #[serde(default = "default_batch_size")]
    pub metrics_batch_size: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_delay")]
    pub retry_delay: u64,
}

fn default_batch_size() -> usize { 1000 }
fn default_max_retries() -> u32 { 3 }
fn default_retry_delay() -> u64 { 5 }

impl Config {
    /// Load configuration from a TOML file
    /// Supports environment variable substitution (e.g., ${VAR_NAME})
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let span = tracing::span!(tracing::Level::DEBUG, "config.load", path = ?path.as_ref());
        let _guard = span.enter();
        
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        
        // Substitute environment variables (${VAR_NAME} format)
        let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
        let content = re.replace_all(&content, |caps: &regex::Captures| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_else(|_| {
                tracing::warn!("Environment variable {} not set, using empty string", var_name);
                String::new()
            })
        }).to_string();
        
        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse TOML config file: {}", path.display()))?;
        
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_load_config() {
        let toml_content = r#"
[api]
url = "https://api.reiver.io"
api_key = "test-key"
timeout = 30

[intervals]
system_metrics = 60
database_metrics = 60
heartbeat = 300

[system_metrics]
enabled = true

[[databases]]
name = "test_db"
type = "postgresql"
enabled = true
host = "localhost"
port = 5432
database = "postgres"
username = "user"
password = "pass"

[logging]
level = "info"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        let path = file.path();

        let config = Config::from_file(path).unwrap();
        assert_eq!(config.api.url, "https://api.reiver.io");
        assert_eq!(config.api.api_key, "test-key");
        assert!(config.system_metrics.enabled);
        assert_eq!(config.databases.len(), 1);
        assert_eq!(config.databases[0].name, "test_db");
    }

    #[test]
    fn test_env_substitution() {
        std::env::set_var("TEST_API_KEY", "secret-key-123");
        
        let toml_content = r#"
[api]
url = "https://api.reiver.io"
api_key = "${TEST_API_KEY}"
timeout = 30

[intervals]
system_metrics = 60
database_metrics = 60
heartbeat = 300

[system_metrics]
enabled = true

[logging]
level = "info"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        let path = file.path();

        let config = Config::from_file(path).unwrap();
        assert_eq!(config.api.api_key, "secret-key-123");
        
        std::env::remove_var("TEST_API_KEY");
    }
}

