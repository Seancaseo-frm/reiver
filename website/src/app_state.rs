use std::sync::Arc;

use reiver_core::app_state::AuthContext;
pub use reiver_core::app_state::RedisPool;
use reiver_core::billing::credits::CreditService;
use reiver_core::billing::BillingService;
use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::config::Config;
use reiver_core::crypto::RotatingSecretEncryptor;
use reiver_core::db::DbPool;
use reiver_core::email::LoopsClient;
use reiver_core::embeddings::KbEmbedder;
use reiver_core::entitlements::EntitlementChecker;

// =============================================================================
// WebsiteState -- Website (auth/identity) product state
// =============================================================================

pub struct WebsiteState {
    pub db: Arc<DbPool>,
    pub clickhouse: Arc<ClickHousePool>,
    pub redis: Arc<RedisPool>,
    pub config: Arc<Config>,
    pub encryptor: Arc<RotatingSecretEncryptor>,
    pub billing: Arc<BillingService>,
    pub credit_service: Arc<CreditService>,
    /// HTTP client for proxying requests to backend services
    pub http_client: reqwest::Client,
    /// Loops.so email client (None when LOOPS_API_KEY is unset)
    pub email: Option<Arc<LoopsClient>>,
    /// Shared Stripe API client (None when STRIPE_API_KEY is unset)
    pub stripe_client: Option<stripe::Client>,
    // Pond disabled — re-enable when Pond launches
    // /// Base URL for the Pond (data warehouse) backend
    // pub pond_url: String,
    /// Base URL for the Flow (LLM gateway) backend
    pub flow_url: String,
    /// Base URL for the Watch (APM) backend
    pub watch_url: String,
    /// Base URL for the MCP server
    pub mcp_url: String,
    /// Base URL for the Herd (A2A) backend
    pub herd_url: String,
    pub entitlements: Arc<dyn EntitlementChecker>,
    /// Local embedding model for knowledge base vector search
    pub kb_embedder: Arc<KbEmbedder>,
}

impl AuthContext for WebsiteState {
    fn db(&self) -> &Arc<DbPool> {
        &self.db
    }
    fn redis(&self) -> &Arc<RedisPool> {
        &self.redis
    }
    fn config(&self) -> &Arc<Config> {
        &self.config
    }
}
