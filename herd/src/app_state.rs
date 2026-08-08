use crate::access_cache::AccessCache;
use crate::clickhouse_db::ClickHousePool;
use crate::config::Config;
use crate::db::DbPool;
use crate::kafka::KafkaProducer;
use crate::routing_cache::RoutingCache;
use std::sync::Arc;

/// Shared application state for the Herd service.
pub struct HerdState {
    pub db: Arc<DbPool>,
    pub clickhouse: ClickHousePool,
    pub kafka: Arc<KafkaProducer>,
    pub config: Arc<Config>,
    pub http_client: reqwest::Client,
    pub herd_enabled: bool,
    pub website_url: String,
    pub access_cache: AccessCache,
    pub routing_cache: Arc<RoutingCache>,
}
