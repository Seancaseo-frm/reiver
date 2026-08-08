pub mod admin;
pub mod chunking;
pub mod alert_rules;
pub mod audit;
pub mod auth_events;
pub mod auth_helpers;
pub mod billing;
pub mod dashboards;
pub mod grafana;
pub mod incidents;
pub mod invitations;
pub mod maintenance_windows;
pub mod mfa;
pub mod migration;
pub mod notification_channels;
pub mod oauth;
pub mod organizations;
pub mod payments;
pub mod projects;
pub mod provisioning;
pub mod scim;
pub mod sso;
pub mod sso_sessions;

pub mod webauthn;

use crate::app_state::WebsiteState;
use axum::Router;
use std::sync::Arc;

/// Create the Website API router with auth/identity/platform routes.
pub fn create_website_api_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .nest("/projects", projects::create_projects_router())
        .nest(
            "/organizations",
            organizations::create_organizations_router(),
        )
        .nest("/billing", billing::create_billing_router())
        .nest("/payments", payments::create_payments_router())
        .nest("/sso", sso::create_sso_router())
        .nest("/sso/sessions", sso_sessions::create_sso_sessions_router())
        .nest("/scim/v2", scim::create_scim_router())
        .nest("/settings/scim", scim::create_scim_settings_router())
        .nest("/provisioning", provisioning::create_provisioning_router())
        .nest("/mfa", mfa::create_mfa_router())
        .nest("/webauthn", webauthn::create_webauthn_router())
        .nest("/auth-events", auth_events::create_auth_events_router())
        .nest("/dashboards", dashboards::create_dashboards_router())
        .nest(
            "/notification-channels",
            notification_channels::create_notification_channels_router(),
        )
        .nest("/alerting", alert_rules::create_alert_rules_router())
        // incidents handlers are called from projects.rs routes directly
        .nest(
            "/maintenance-windows",
            maintenance_windows::create_maintenance_windows_router(),
        )
        .nest("/admin", admin::create_admin_router())
        .nest("/audit", audit::create_audit_router())
        .nest("/org/invitations", invitations::create_invitations_router())

}
