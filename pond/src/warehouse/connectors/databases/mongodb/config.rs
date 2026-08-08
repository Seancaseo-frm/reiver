//! MongoDB Connector Configuration
//!
//! Configuration for connecting to MongoDB databases and optional ClickHouse index layer.

use std::time::Duration;

/// MongoDB read preference for replica set reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadPreference {
    /// Read from the primary replica.
    Primary,
    /// Read from secondary replicas only.
    Secondary,
    /// Prefer reading from secondary replicas, fall back to primary.
    #[default]
    SecondaryPreferred,
    /// Read from the nearest replica (lowest latency).
    Nearest,
}

impl ReadPreference {
    /// Convert to mongodb driver's ReadPreference.
    pub fn to_mongodb_read_preference(&self) -> mongodb::options::ReadPreference {
        match self {
            ReadPreference::Primary => mongodb::options::ReadPreference::Primary,
            ReadPreference::Secondary => {
                mongodb::options::ReadPreference::Secondary { options: None }
            }
            ReadPreference::SecondaryPreferred => {
                mongodb::options::ReadPreference::SecondaryPreferred { options: None }
            }
            ReadPreference::Nearest => {
                mongodb::options::ReadPreference::Nearest { options: None }
            }
        }
    }
}

/// MongoDB connector configuration.
#[derive(Debug, Clone)]
pub struct MongoDBConfig {
    /// MongoDB connection string (e.g., "mongodb://localhost:27017")
    pub connection_string: String,
    /// Database name to connect to
    pub database: String,
    /// Collections to expose (empty = all collections)
    pub collections: Vec<String>,
    /// Cache TTL for schema and metadata in seconds
    pub cache_ttl_secs: u64,
    /// Number of documents to fetch per batch during streaming
    pub batch_size: usize,
    /// Read preference for replica set reads
    pub read_preference: ReadPreference,
    /// Maximum depth for nested document flattening (deeper levels become JSON strings)
    pub max_nested_depth: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Server selection timeout
    pub server_selection_timeout: Duration,
    /// Enable ClickHouse index layer for accelerated queries
    pub index_enabled: bool,
    /// ClickHouse database for index tables (if index_enabled)
    pub index_database: Option<String>,
    /// Number of documents to sample for schema inference
    pub schema_sample_size: usize,
}

impl Default for MongoDBConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            database: String::new(),
            collections: Vec::new(),
            cache_ttl_secs: 300, // 5 minutes
            batch_size: 10_000,
            read_preference: ReadPreference::default(),
            max_nested_depth: 3,
            connect_timeout: Duration::from_secs(10),
            server_selection_timeout: Duration::from_secs(30),
            index_enabled: false,
            index_database: None,
            schema_sample_size: 100,
        }
    }
}

impl MongoDBConfig {
    /// Create a new MongoDB configuration.
    ///
    /// # Arguments
    /// * `connection_string` - MongoDB connection URI
    /// * `database` - Database name
    pub fn new(connection_string: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            database: database.into(),
            ..Default::default()
        }
    }

    /// Set collections to expose (empty = all collections).
    pub fn with_collections(mut self, collections: Vec<String>) -> Self {
        self.collections = collections;
        self
    }

    /// Set cache TTL in seconds.
    pub fn with_cache_ttl(mut self, ttl_secs: u64) -> Self {
        self.cache_ttl_secs = ttl_secs;
        self
    }

    /// Set batch size for streaming fetches.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set read preference for replica set reads.
    pub fn with_read_preference(mut self, read_preference: ReadPreference) -> Self {
        self.read_preference = read_preference;
        self
    }

    /// Set maximum depth for nested document flattening.
    pub fn with_max_nested_depth(mut self, depth: usize) -> Self {
        self.max_nested_depth = depth;
        self
    }

    /// Enable ClickHouse index layer.
    ///
    /// # Arguments
    /// * `index_database` - ClickHouse database name for index tables
    pub fn with_index(mut self, index_database: impl Into<String>) -> Self {
        self.index_enabled = true;
        self.index_database = Some(index_database.into());
        self
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set server selection timeout.
    pub fn with_server_selection_timeout(mut self, timeout: Duration) -> Self {
        self.server_selection_timeout = timeout;
        self
    }

    /// Set the number of documents to sample for schema inference.
    pub fn with_schema_sample_size(mut self, sample_size: usize) -> Self {
        self.schema_sample_size = sample_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = MongoDBConfig::new("mongodb://localhost:27017", "mydb");
        assert_eq!(config.connection_string, "mongodb://localhost:27017");
        assert_eq!(config.database, "mydb");
        assert!(config.collections.is_empty());
        assert_eq!(config.batch_size, 10_000);
        assert!(!config.index_enabled);
    }

    #[test]
    fn test_config_with_collections() {
        let config = MongoDBConfig::new("mongodb://localhost", "mydb")
            .with_collections(vec!["users".to_string(), "orders".to_string()]);
        assert_eq!(config.collections.len(), 2);
    }

    #[test]
    fn test_config_with_index() {
        let config = MongoDBConfig::new("mongodb://localhost", "mydb")
            .with_index("reiver_indexes");
        assert!(config.index_enabled);
        assert_eq!(config.index_database, Some("reiver_indexes".to_string()));
    }

    #[test]
    fn test_read_preference_default() {
        let config = MongoDBConfig::default();
        assert_eq!(config.read_preference, ReadPreference::SecondaryPreferred);
    }

    #[test]
    fn test_read_preference_conversion() {
        let pref = ReadPreference::Primary;
        let mongo_pref = pref.to_mongodb_read_preference();
        assert!(matches!(mongo_pref, mongodb::options::ReadPreference::Primary));
    }
}
