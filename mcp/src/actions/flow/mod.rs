pub mod integrations;
pub mod metrics;
pub mod playground;
pub mod pricing;
pub mod prompts;
pub mod scores;
pub mod search;
pub mod session_profiles;
pub mod sessions;
pub mod settings;

use crate::registry::ActionRegistry;

pub fn register(registry: &mut ActionRegistry) {
    prompts::register(registry);
    playground::register(registry);
    metrics::register(registry);
    sessions::register(registry);
    session_profiles::register(registry);
    integrations::register(registry);
    settings::register(registry);
    scores::register(registry);
    search::register(registry);
    pricing::register(registry);
}
