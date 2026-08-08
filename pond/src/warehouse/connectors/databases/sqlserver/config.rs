//! SQL Server Connector Configuration
//!
//! Configuration for connecting to Microsoft SQL Server databases.

/// SQL Server connector configuration.
#[derive(Debug, Clone)]
pub struct SqlServerConfig {
    /// SQL Server host address
    pub host: String,
    /// SQL Server port (default: 1433)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: String,
    /// Whether to trust the server certificate (useful for self-signed certs)
    pub trust_server_certificate: bool,
    /// Tables to sync (empty = all tables in dbo schema)
    pub tables: Vec<String>,
    /// Maximum connections in the pool
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Schema to query (default: "dbo")
    pub schema: String,
    /// Whether CDC (Change Data Capture) sync is enabled
    pub cdc_enabled: bool,
    /// CDC poll interval in seconds
    pub cdc_poll_interval_secs: u64,
    /// ClickHouse database for index tables (if using index acceleration)
    pub index_database: Option<String>,
}

impl SqlServerConfig {
    /// Create a new SQL Server configuration.
    ///
    /// # Arguments
    /// * `host` - SQL Server host address
    /// * `database` - Database name
    /// * `username` - Username for authentication
    /// * `password` - Password for authentication
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 1433,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            trust_server_certificate: false,
            tables: Vec::new(),
            max_connections: 5,
            connect_timeout_secs: 30,
            schema: "dbo".to_string(),
            cdc_enabled: false,
            cdc_poll_interval_secs: 5,
            index_database: None,
        }
    }

    /// Set the port (default: 1433).
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Trust the server certificate (for self-signed certificates).
    pub fn with_trust_server_certificate(mut self, trust: bool) -> Self {
        self.trust_server_certificate = trust;
        self
    }

    /// Set specific tables to sync (empty = all tables).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Set maximum connections in the pool.
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set connection timeout in seconds.
    pub fn with_connect_timeout(mut self, timeout_secs: u64) -> Self {
        self.connect_timeout_secs = timeout_secs;
        self
    }

    /// Set the schema to query (default: "dbo").
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Enable CDC (Change Data Capture) sync.
    pub fn with_cdc(mut self, enabled: bool) -> Self {
        self.cdc_enabled = enabled;
        self
    }

    /// Set CDC poll interval in seconds.
    pub fn with_cdc_poll_interval(mut self, interval_secs: u64) -> Self {
        self.cdc_poll_interval_secs = interval_secs;
        self
    }

    /// Set the ClickHouse database for index tables.
    pub fn with_index_database(mut self, database: impl Into<String>) -> Self {
        self.index_database = Some(database.into());
        self
    }

    /// Build the tiberius connection configuration.
    pub fn to_tiberius_config(&self) -> tiberius::Config {
        let mut config = tiberius::Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.authentication(tiberius::AuthMethod::sql_server(&self.username, &self.password));
        
        if self.trust_server_certificate {
            config.trust_cert();
        }
        
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = SqlServerConfig::new("localhost", "testdb", "sa", "password123");
        
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
        assert_eq!(config.database, "testdb");
        assert_eq!(config.username, "sa");
        assert_eq!(config.schema, "dbo");
        assert!(!config.cdc_enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = SqlServerConfig::new("server.example.com", "mydb", "user", "pass")
            .with_port(1434)
            .with_trust_server_certificate(true)
            .with_tables(vec!["users".to_string(), "orders".to_string()])
            .with_max_connections(10)
            .with_schema("sales")
            .with_cdc(true)
            .with_cdc_poll_interval(10)
            .with_index_database("reiver_indexes");

        assert_eq!(config.port, 1434);
        assert!(config.trust_server_certificate);
        assert_eq!(config.tables.len(), 2);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.schema, "sales");
        assert!(config.cdc_enabled);
        assert_eq!(config.cdc_poll_interval_secs, 10);
        assert_eq!(config.index_database, Some("reiver_indexes".to_string()));
    }
}
