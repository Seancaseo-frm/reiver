//! Shared request context extracted from HTTP headers.
//!
//! `RequestContext` captures the cross-cutting metadata that every gateway
//! endpoint needs (project identity, billing attribution, request tracing).
//! Provider resolution and billing gates are methods on the context so that
//! both chat completions and embeddings (and future endpoints) share them.

use axum::http::HeaderMap;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::gateway::error::GatewayError;
use crate::gateway::provider_manager::{ProviderKeyStore, ResolvedKey};
use crate::gateway::provider_types::Provider;

/// Parsed per-request metadata shared across all gateway endpoints.
#[derive(Debug)]
pub(crate) struct RequestContext {
    pub project_id: Uuid,
    pub billing_project_id: Uuid,
    pub request_id: String,
}

/// A provider + API key pair resolved for a specific model.
pub(crate) struct ResolvedProvider {
    pub provider: Provider,
    pub key: ResolvedKey,
}

impl RequestContext {
    /// Build a `RequestContext` from the inbound HTTP headers.
    ///
    /// `X-Project-Id` is required; `X-Billing-Project-Id` defaults to the
    /// project id when absent. A v7 UUID request id is always generated.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, GatewayError> {
        let project_id = crate::api::extract_project_id(headers).map_err(|_| {
            GatewayError::AuthenticationFailed(
                "Missing or invalid X-Project-Id header".to_string(),
            )
        })?;

        let billing_project_id = headers
            .get("X-Billing-Project-Id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or(project_id);

        let request_id = Uuid::now_v7().to_string();

        Ok(Self {
            project_id,
            billing_project_id,
            request_id,
        })
    }

    /// Resolve the LLM provider and API key for a given model name.
    ///
    /// Uses `Provider::from_model_prefix` to identify the provider, then
    /// fetches the project's API key via the `ProviderKeyStore`.
    pub async fn resolve_provider_and_key(
        &self,
        state: &FlowState,
        model: &str,
    ) -> Result<ResolvedProvider, GatewayError> {
        let provider = Provider::from_model_prefix(model).ok_or_else(|| {
            GatewayError::UnsupportedModel(model.to_string())
        })?;

        let key = state
            .get_key(self.project_id, provider)
            .await
            .ok_or_else(|| {
                GatewayError::MissingProviderKey(format!(
                    "API key not configured for provider '{}'",
                    provider
                ))
            })?;

        Ok(ResolvedProvider { provider, key })
    }

    /// Run pre-flight billing gates.
    ///
    /// Only enforced when `state.credits_enabled` is true. Checks:
    /// 1. Organization is resolved (org_id is Some)
    /// 2. Organization has an active subscription
    /// 3. Organization has a payment method OR Stripe credit balance
    ///
    /// Stripe handles actual billing: credits are consumed first, then the card
    /// is charged for the remainder. Exhaustion is handled via failed invoices
    /// and Stripe's dunning system.
    pub async fn check_billing_gates(
        &self,
        state: &FlowState,
        org_id: Option<Uuid>,
        _is_platform_key: bool,
    ) -> Result<(), GatewayError> {
        if !state.credits_enabled {
            return Ok(());
        }

        let oid = match org_id {
            Some(id) => id,
            None => return Err(GatewayError::PaymentRequired),
        };

        if !state.check_has_active_subscription(oid).await {
            return Err(GatewayError::PaymentRequired);
        }

        let has_pm = state.check_has_payment_method(oid).await;
        if has_pm {
            return Ok(());
        }

        let has_credits = state.check_has_stripe_credits(oid).await;
        if has_credits {
            return Ok(());
        }

        Err(GatewayError::PaymentRequired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn header_map_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    #[test]
    fn test_from_headers_valid() {
        let pid = Uuid::now_v7();
        let bpid = Uuid::now_v7();
        let headers = header_map_with(&[
            ("x-project-id", &pid.to_string()),
            ("x-billing-project-id", &bpid.to_string()),
        ]);
        let ctx = RequestContext::from_headers(&headers).unwrap();
        assert_eq!(ctx.project_id, pid);
        assert_eq!(ctx.billing_project_id, bpid);
        assert!(!ctx.request_id.is_empty());
    }

    #[test]
    fn test_from_headers_missing_project_id() {
        let headers = HeaderMap::new();
        let err = RequestContext::from_headers(&headers).unwrap_err();
        assert!(
            matches!(err, GatewayError::AuthenticationFailed(_)),
            "expected AuthenticationFailed, got {:?}",
            err
        );
    }

    #[test]
    fn test_from_headers_malformed_uuid() {
        let headers = header_map_with(&[("x-project-id", "not-a-uuid")]);
        let err = RequestContext::from_headers(&headers).unwrap_err();
        assert!(matches!(err, GatewayError::AuthenticationFailed(_)));
    }

    #[test]
    fn test_from_headers_billing_defaults_to_project() {
        let pid = Uuid::now_v7();
        let headers = header_map_with(&[("x-project-id", &pid.to_string())]);
        let ctx = RequestContext::from_headers(&headers).unwrap();
        assert_eq!(ctx.billing_project_id, pid);
    }

    #[test]
    fn test_from_headers_generates_request_id() {
        let pid = Uuid::now_v7();
        let headers = header_map_with(&[("x-project-id", &pid.to_string())]);
        let ctx = RequestContext::from_headers(&headers).unwrap();
        assert!(
            Uuid::parse_str(&ctx.request_id).is_ok(),
            "request_id should be a valid UUID, got: {}",
            ctx.request_id
        );
    }
}
