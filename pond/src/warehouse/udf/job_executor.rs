use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobConfig {
    pub source: JobSourceConfig,
    pub sink: JobSinkConfig,
    #[serde(default)]
    pub parallelism: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobSourceConfig {
    pub name: String,
    pub table: Option<String>,
    pub filter: Option<String>,
    pub query: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobSinkConfig {
    pub name: String,
    pub table: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_source_config_parses_with_query() {
        let json = serde_json::json!({
            "source": {
                "name": "pg",
                "query": "SELECT * FROM users WHERE active = true"
            },
            "sink": {
                "name": "ch",
                "table": "users_transformed"
            }
        });
        let config: JobConfig = serde_json::from_value(json).unwrap();
        assert!(config.source.query.is_some());
        assert!(config.source.table.is_none());
        assert_eq!(
            config.source.query.as_deref().unwrap(),
            "SELECT * FROM users WHERE active = true"
        );
    }

    #[test]
    fn job_source_config_parses_with_filter() {
        let json = serde_json::json!({
            "source": {
                "name": "pg",
                "table": "orders",
                "filter": "status = 'active'"
            },
            "sink": {
                "name": "ch",
                "table": "orders_out"
            }
        });
        let config: JobConfig = serde_json::from_value(json).unwrap();
        assert!(config.source.filter.is_some());
        assert!(config.source.table.is_some());
        assert_eq!(config.source.filter.as_deref().unwrap(), "status = 'active'");
    }

    #[test]
    fn job_source_config_parses_with_table_only() {
        let json = serde_json::json!({
            "source": {
                "name": "pg",
                "table": "orders"
            },
            "sink": {
                "name": "ch",
                "table": "orders_out"
            }
        });
        let config: JobConfig = serde_json::from_value(json).unwrap();
        assert!(config.source.query.is_none());
        assert!(config.source.filter.is_none());
        assert_eq!(config.source.table.as_deref().unwrap(), "orders");
    }

    #[test]
    fn job_source_config_requires_name() {
        let json = serde_json::json!({
            "source": {
                "table": "orders"
            },
            "sink": {
                "name": "ch",
                "table": "orders_out"
            }
        });
        let result: Result<JobConfig, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn source_routing_picks_query_path() {
        let config = JobSourceConfig {
            name: "pg".to_string(),
            table: None,
            filter: None,
            query: Some("SELECT 1".to_string()),
        };
        assert!(config.query.is_some());
        assert!(config.table.is_none());
    }

    #[test]
    fn source_routing_picks_filter_path() {
        let config = JobSourceConfig {
            name: "pg".to_string(),
            table: Some("orders".to_string()),
            filter: Some("id > 10".to_string()),
            query: None,
        };
        assert!(config.filter.is_some());
        assert!(config.table.is_some());
        assert!(config.query.is_none());
    }

    #[test]
    fn source_routing_filter_without_table_detected() {
        let config = JobSourceConfig {
            name: "pg".to_string(),
            table: None,
            filter: Some("id > 10".to_string()),
            query: None,
        };
        assert!(config.filter.is_some());
        assert!(config.table.is_none());
    }
}
