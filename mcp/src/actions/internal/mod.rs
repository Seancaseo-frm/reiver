pub mod llm_pricing;
pub mod web_search;

use crate::registry::ActionRegistry;

pub fn register(registry: &mut ActionRegistry) {
    web_search::register(registry);
    llm_pricing::register(registry);
}
