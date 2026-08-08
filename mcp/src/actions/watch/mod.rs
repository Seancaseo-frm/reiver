pub mod api_endpoints;
pub mod auth_events;
pub mod aws;
pub mod azure;
pub mod exceptions;
pub mod gcp;
pub mod github;
pub mod health_checks;
pub mod incidents;
pub mod logs;
pub mod maintenance_windows;
pub mod metrics;
pub mod notification_channels;
pub mod oci;
pub mod profiles;
pub mod services;
pub mod system_overview;
pub mod traces;

use crate::registry::ActionRegistry;

pub fn register(registry: &mut ActionRegistry) {
    exceptions::register(registry);
    traces::register(registry);
    logs::register(registry);
    incidents::register(registry);
    services::register(registry);
    metrics::register(registry);
    notification_channels::register(registry);
    aws::register(registry);
    azure::register(registry);
    gcp::register(registry);
    oci::register(registry);
    auth_events::register(registry);
    health_checks::register(registry);
    maintenance_windows::register(registry);
    profiles::register(registry);
    api_endpoints::register(registry);
    github::register(registry);
    system_overview::register(registry);
}
