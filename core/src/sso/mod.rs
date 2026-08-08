//! SSO (Single Sign-On) infrastructure
//!
//! Provides:
//! - Session management with tracking and revocation
//! - JIT (Just-In-Time) provisioning rules
//! - Audit logging for SSO events

pub mod provisioning;
pub mod sessions;

pub use provisioning::{ProvisioningAction, ProvisioningEngine, ProvisioningRule};
pub use sessions::{RevocationReason, SessionManager, SsoSession};
