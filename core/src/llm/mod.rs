//! LLM Observability Module (shared core)
//!
//! Provides AI/LLM monitoring capabilities based on OpenTelemetry GenAI semantic conventions.
//! Shared between Watch (APM monitoring) and Flow (gateway traffic recording).

pub mod cache;
pub mod cost;
pub mod helpers;
pub mod processor;
pub mod template;
pub mod types;

pub use cache::invalidate_rollout_cache;
pub use cost::CostCalculator;
pub use helpers::register_builtin_helpers;
pub use processor::LlmSpanProcessor;
pub use template::compile_prompt;
pub use types::*;
