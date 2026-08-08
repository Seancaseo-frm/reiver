pub mod alert_rules;
pub mod auth_helpers;
pub mod aws;
pub mod azure;
pub mod dashboards;
pub mod database_monitoring;
pub mod discord;
pub mod events;
pub mod events_storage;
pub mod exceptions;
pub mod flamegraph;
pub mod gcp;
pub mod health_checks;
pub mod historical;
pub mod incidents;
pub mod logs;
pub mod maintenance_windows;
pub mod metrics;
pub mod monitoring;
pub mod notification_channels;
pub mod oci;
pub mod otlp;
pub mod pagerduty;
pub mod profiles;
pub mod projects;
pub mod scheduled_events;
pub mod servicenow;
pub mod slack;
pub mod spans;
pub mod teams;
pub mod system_overview;
pub mod widget_query;
pub mod xray;

// GitHub integration
pub mod github;

// Game Observability
pub mod game_analytics;
pub mod infra;

// MCP / Agent tool analytics
pub mod mcp_tools;

use crate::app_state::WatchState;
use crate::config::Config;
use crate::error::{AppError, Result};
use axum::http::HeaderMap;
use axum::Router;
use std::sync::Arc;
use uuid::Uuid;

/// Extract the authenticated user ID from the trusted `X-User-Id` header.
///
/// The website gateway validates the JWT before forwarding the request
/// here with this header. Watch trusts it.
pub fn extract_user_id(headers: &HeaderMap) -> Result<Uuid> {
    headers
        .get("X-User-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Auth("Missing or invalid X-User-Id header".to_string()))
}

/// Extract the project ID from the trusted `X-Project-Id` header.
///
/// The website gateway validates the API key / project access and resolves
/// the project before forwarding the request here with this header.
pub fn extract_project_id(headers: &HeaderMap) -> Result<Uuid> {
    headers
        .get("X-Project-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| AppError::Auth("Missing or invalid X-Project-Id header".to_string()))
}

/// Extract the project ID from `X-Project-Id` if present, returning `None` otherwise.
pub fn extract_project_id_optional(headers: &HeaderMap) -> Option<Uuid> {
    headers
        .get("X-Project-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// Create the Watch (APM) API router with all APM-specific routes.
/// Identity/auth/billing routes are served by the Website project.
pub fn create_watch_api_router(config: &Config) -> Router<Arc<WatchState>> {
    Router::new()
        .nest("/exceptions", exceptions::create_exceptions_router())
        .nest("/spans", spans::create_spans_router()) // DEPRECATED: Use /v1/traces instead
        .nest(
            "/v1",
            otlp::create_otlp_router().merge(events::create_events_router()),
        ) // OTLP standard endpoints: /v1/traces, /v1/logs, /v1/metrics, /v1/profiles
        .nest("/query/metrics", metrics::create_metrics_router()) // Custom metrics query API at /api/query/metrics/*
        .nest(
            "/database-monitoring",
            database_monitoring::create_database_monitoring_router(),
        )
        .nest("/historical", historical::create_historical_router()) // Polling endpoints for historical charts
        .nest("/monitoring", monitoring::create_monitoring_router()) // Monitoring endpoints (Kafka lag, ClickHouse ingestion)
        .nest("/profiles", profiles::create_profiles_router()) // Profiles API endpoints
        .nest("/aws", aws::create_aws_router()) // AWS integrations API endpoints
        .nest("/azure", azure::create_azure_router()) // Azure integrations API endpoints
        .nest("/gcp", gcp::create_gcp_router()) // GCP integrations API endpoints
        .nest("/oci", oci::create_oci_router()) // OCI integrations API endpoints
        .nest(
            "/notification-channels",
            notification_channels::create_notification_channels_router(),
        ) // Unified notification channels
        .nest("/discord", discord::create_discord_router()) // Discord webhook integrations
        .nest("/slack", slack::create_slack_router()) // Slack webhook integrations
        .nest("/pagerduty", pagerduty::create_pagerduty_router()) // PagerDuty integrations
        .nest("/teams", teams::create_teams_router()) // Microsoft Teams integrations
        .nest("/servicenow", servicenow::create_servicenow_router()) // ServiceNow (different - has auth)
        .nest("/xray", xray::create_xray_router()) // AWS X-Ray trace ingestion endpoints
        .nest(
            "/health-checks",
            health_checks::create_health_checks_router(),
        ) // Synthetic monitoring
        .nest("/alerting", alert_rules::create_alert_rules_router()) // Alert rules and alerts
        .nest(
            "/maintenance-windows",
            maintenance_windows::create_maintenance_windows_router(),
        ) // Planned maintenance
        .nest("/events", events::create_events_router()) // Feature flag change events
        .nest("/logs", logs::create_logs_router()) // Log ingestion endpoints
        .merge(widget_query::create_widget_query_router()) // Widget query execution
        .nest("/system-overview", system_overview::create_system_overview_router())
        // GitHub integration
        .nest("/github", github::create_github_router(config))
        // Game Observability
        .nest("/game", game_analytics::create_game_analytics_router())
        // Internal: emit scheduled events (called by K8s CronJob)
        .nest(
            "/internal",
            scheduled_events::create_scheduled_events_router(),
        )
}

/// Create the Watch (APM) projects router.
/// Note: LLM integration/settings routes are served by Flow via nginx proxy.
pub fn create_watch_projects_router(config: &Config) -> Router<Arc<WatchState>> {
    projects::create_projects_router(config)
}
