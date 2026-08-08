pub mod checker;
pub mod mock;
pub mod service;
pub mod types;
pub mod usage_enforcer;

pub use checker::{EntitlementChecker, UnlimitedEntitlements};
pub use service::EntitlementService;
pub use types::{Product, ResolvedTier, TierConfig};
pub use usage_enforcer::{UsageEnforcer, UsageGate};

#[cfg(any(test, feature = "test-utils"))]
pub use mock::MockEntitlementChecker;
