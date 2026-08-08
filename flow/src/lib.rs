//! Reiver Flow -- LLM Gateway
//!
//! This crate provides the LLM Gateway product:
//! - AI Gateway router (OpenAI-compatible API)
//! - LLM observability and cost tracking
//! - Prompt management and rollouts
//! - Semantic caching

// Re-export core modules so `use crate::X` works for core types.
// Auth, rate limiting, billing, SSO, SAML, MFA, and audit are website concerns
// and not re-exported here. Flow is an internal service that trusts the website proxy.
pub use reiver_core::{
    alerts, audit, clickhouse_db, config, crypto, db, error, intern, kafka, llm, models, pii, pool,
    query_cache, storage, types, utils,
};

// Re-export app_state module
pub mod app_state;

// Flow domain logic
pub mod gateway;

// Moodeng internal LLM client (uses platform key, bills to user project)
pub mod moodeng;

pub mod rollout_worker;

// Flow API handlers
pub mod api;

// Trusted proxy enforcement middleware
pub mod trusted_proxy;

// OpenTelemetry initialization (traces, metrics, logs to Watch)
pub mod telemetry;

// OTel metrics instruments for the gateway
pub mod metrics;

// OpenRouter model catalog parser
pub mod openrouter_catalog;
