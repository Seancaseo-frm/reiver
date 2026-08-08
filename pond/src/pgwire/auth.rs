//! Custom StartupHandler for project API key authentication.
//!
//! BI tools authenticate by passing a project API key as the Postgres username.
//! The password field is accepted but ignored -- only the username (API key)
//! is validated via `validate_project_key_cached()`.
//!
//! On success, the validated `project_id` (UUID) is stored in the connection's
//! metadata map for all subsequent query handlers to use.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use pgwire::api::auth::{
    DefaultServerParameterProvider, StartupHandler,
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, METADATA_USER};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::app_state::PondState;
use crate::pgwire::handler::SESSION_PREFIX;
use crate::pgwire::session::{default_value_for, DEFAULT_SESSION_KEYS};
use reiver_core::utils::validate_project_key_cached;

/// Metadata key for storing the validated project ID on the connection.
pub const METADATA_PROJECT_ID: &str = "project_id";

/// Startup handler that authenticates connections using project API keys.
///
/// Flow:
/// 1. Client sends Startup with `user=<project-api-key>`
/// 2. Server responds with CleartextPassword request
/// 3. Client sends any password (ignored)
/// 4. Server validates the username via `validate_project_key_cached`
/// 5. On success: stores `project_id` in metadata, completes auth
/// 6. On failure: returns Postgres FATAL error, connection drops
pub struct ProjectKeyStartupHandler {
    state: Arc<PondState>,
    parameter_provider: DefaultServerParameterProvider,
}

impl ProjectKeyStartupHandler {
    pub fn new(state: Arc<PondState>) -> Self {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "16.6-reiver-pond".to_owned();
        Self {
            state,
            parameter_provider: params,
        }
    }
}

#[async_trait]
impl StartupHandler for ProjectKeyStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);

                // Request cleartext password (BI tools expect a password prompt)
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(_pwd) => {
                // Ignore the password -- we validate the username as a project API key.
                let api_key = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .ok_or_else(|| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28000".to_owned(),
                            "No username (project API key) provided".to_owned(),
                        )))
                    })?;

                let project_id = validate_project_key_cached(
                    &self.state.redis,
                    &self.state.db,
                    &api_key,
                )
                .await
                .map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28P01".to_owned(),
                        "Invalid project API key".to_owned(),
                    )))
                })?;

                // Store project_id in connection metadata for query handlers
                client
                    .metadata_mut()
                    .insert(METADATA_PROJECT_ID.to_owned(), project_id.to_string());

                tracing::info!(
                    project_id = %project_id,
                    client_addr = %client.socket_addr(),
                    "PgWire client authenticated"
                );

                finish_authentication(client, &self.parameter_provider).await?;

                // Seed default session parameters into client metadata so that
                // SHOW queries work immediately without explicit SET.
                for key in DEFAULT_SESSION_KEYS {
                    if let Some(val) = default_value_for(key) {
                        client
                            .metadata_mut()
                            .insert(format!("{}{}", SESSION_PREFIX, key), val.to_owned());
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}
