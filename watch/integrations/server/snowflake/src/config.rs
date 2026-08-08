//! Configuration for Snowflake integrations

use serde::{Deserialize, Serialize};

/// Snowflake integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnowflakeConfig {
    /// Snowflake account identifier (e.g., "xy12345")
    pub account: String,
    
    /// Snowflake username
    pub username: String,
    
    /// Snowflake password (in production, this should be encrypted)
    pub password: String,
    
    /// Snowflake warehouse name
    pub warehouse: Option<String>,
    
    /// Snowflake database name
    pub database: Option<String>,
    
    /// Snowflake schema name
    pub schema: Option<String>,
    
    /// Snowflake role (optional)
    pub role: Option<String>,
}

impl Default for SnowflakeConfig {
    fn default() -> Self {
        Self {
            account: String::new(),
            username: String::new(),
            password: String::new(),
            warehouse: None,
            database: None,
            schema: None,
            role: None,
        }
    }
}
