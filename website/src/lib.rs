//! Reiver Website -- Frontend + Auth/Identity Backend
//!
//! This crate provides:
//! - User authentication (login, registration, JWT)
//! - Organization and project management
//! - Billing and payments
//! - SSO/SAML integration
//! - MFA (TOTP, WebAuthn)
//! - Dashboard and alert configuration
//! - Frontend serving

// Re-export core modules so `use crate::X` works for shared types
pub use reiver_core::{
    alerts,
    audit,
    // Auth & identity modules
    auth,
    authorization,
    billing,
    clickhouse_db,
    config,
    crypto,
    db,
    error,
    intern,
    kafka,
    mfa,
    models,
    pii,
    pool,
    query_cache,
    rate_limit,
    saml,
    sso,
    storage,
    types,
    utils,
};

// Website state
pub mod app_state;

// Website auth routes (signup, login, get_me)
pub mod auth_routes;

pub mod platform_settings;

pub mod ingestion_stress;

// Shared helpers
pub mod maintenance;
pub mod worker;

// Reverse proxy for backend services
pub mod proxy;

// Website API handlers
pub mod api;

// Website workers
pub mod auth_event_worker;
pub mod billing_worker;
pub mod sso_worker;
