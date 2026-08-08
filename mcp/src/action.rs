use async_trait::async_trait;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::client::InternalClient;

/// Identifies who is making the request.
#[derive(Debug, Clone)]
pub enum Caller {
    ApiKey {
        key_id: Uuid,
    },
    User {
        user_id: Uuid,
        /// Forwarded JWT from the website proxy so tool calls back to the
        /// website can authenticate as the original user.
        jwt: String,
    },
    /// System-initiated request (e.g. auto-investigation triggered by an alert).
    System,
}

/// Shared context available to every action invocation.
///
/// When running inside the Flow agent loop the `db` and `encryptor` fields
/// are populated so actions can resolve secret slots in-process. The
/// standalone MCP binary leaves them as `None`.
#[derive(Clone)]
pub struct ActionContext {
    pub project_id: Uuid,
    pub caller: Caller,
    pub scopes: Vec<String>,
    pub http: InternalClient,
    /// Postgres pool — set when running in-process inside Flow.
    pub db: Option<sqlx::PgPool>,
    /// ClickHouse pool — set when running in-process inside Flow.
    pub clickhouse: Option<reiver_core::clickhouse_db::ClickHousePool>,
    /// Secret encryptor — set when running in-process inside Flow.
    pub encryptor: Option<std::sync::Arc<reiver_core::crypto::RotatingSecretEncryptor>>,
    /// Asset storage for reading file attachments — set when running in-process inside Flow.
    pub asset_storage: Option<std::sync::Arc<dyn reiver_core::storage::AssetStorage>>,
    /// Local embedding model for knowledge base vector similarity search.
    pub kb_embedder: Option<std::sync::Arc<reiver_core::embeddings::KbEmbedder>>,
    /// Meter service for billing. None in standalone MCP binary without Stripe.
    pub meter_service: Option<reiver_core::billing::MeterService>,
    /// Organization ID for billing attribution (resolved from project).
    pub organization_id: Option<Uuid>,
    /// Entitlements checker for cached tier config lookups.
    pub entitlements: std::sync::Arc<dyn reiver_core::entitlements::EntitlementChecker>,
    /// First 4 hex chars of the hashed key, for attribution in analytics.
    pub key_prefix: String,
    /// Human-readable label assigned to this key.
    pub key_label: String,
}

/// A strongly-typed platform action.
///
/// Each action defines concrete `Input` and `Output` types. The only place
/// `serde_json::Value` conversion happens is inside the [`ActionRegistry`]
/// type-erasure layer -- action implementations never touch it.
#[async_trait]
pub trait PlatformAction: Send + Sync + 'static {
    type Input: DeserializeOwned + JsonSchema + Send;
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn required_scope(&self) -> String;

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output>;
}
