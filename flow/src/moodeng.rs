//! Moodeng internal LLM client.
//!
//! All platform-initiated LLM calls (agent chat, prompt compiler, LLM-as-judge)
//! route through [`MoodengClient`] so that:
//!
//! 1. The gateway resolves API keys from the **Moodeng platform project**
//!    (`MOODENG_PROJECT_ID`).
//! 2. Billing (credit deductions, BYOK fees, usage metadata) is attributed to
//!    the **user's project** via the `X-Billing-Project-Id` header.

use uuid::Uuid;

use crate::api::gateway_client::{
    call_gateway, call_gateway_stream, GatewayCallError, GatewayCallResult, GatewayStreamResult,
};
use crate::app_state::FlowState;
use crate::gateway::types::ChatCompletionRequest;

/// Thin wrapper around the gateway client that always uses the Moodeng
/// platform API key while attributing billing to a specific user project.
pub struct MoodengClient<'a> {
    state: &'a FlowState,
    billing_project_id: Uuid,
}

impl<'a> MoodengClient<'a> {
    pub fn new(state: &'a FlowState, billing_project_id: Uuid) -> Self {
        Self {
            state,
            billing_project_id,
        }
    }

    /// Access the underlying application state (DB, caches, etc.).
    pub fn state(&self) -> &FlowState {
        self.state
    }

    /// The project ID sent as `X-Project-Id` for API key resolution.
    /// Falls back to the billing project when `MOODENG_PROJECT_ID` is unset.
    pub fn key_project_id(&self) -> Uuid {
        self.state
            .moodeng_project_id
            .unwrap_or(self.billing_project_id)
    }

    pub fn billing_project_id(&self) -> Uuid {
        self.billing_project_id
    }

    fn flow_url(&self) -> &str {
        &self.state.internal_urls.flow
    }

    /// Non-streaming gateway call using the platform key.
    pub async fn call_llm(
        &self,
        request: &ChatCompletionRequest,
        session_id: Option<&str>,
    ) -> Result<GatewayCallResult, GatewayCallError> {
        call_gateway(
            &self.state.agent_http_client,
            self.flow_url(),
            self.key_project_id(),
            request,
            session_id,
            Some(self.billing_project_id),
        )
        .await
    }

    /// Streaming gateway call using the platform key.
    pub async fn call_llm_stream(
        &self,
        request: &ChatCompletionRequest,
        session_id: Option<&str>,
    ) -> Result<GatewayStreamResult, GatewayCallError> {
        call_gateway_stream(
            &self.state.agent_http_client,
            self.flow_url(),
            self.key_project_id(),
            request,
            session_id,
            Some(self.billing_project_id),
        )
        .await
    }
}
