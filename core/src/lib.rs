//! Reiver Core
//!
//! Shared infrastructure used by all product binaries:
//! - reiver-watch (APM)
//! - reiver-flow (LLM Gateway)
//! - reiver-pond (Data Warehouse)

// Database & messaging
pub mod clickhouse_db;
pub mod config;
pub mod crypto;
pub mod db;
pub mod kafka;

// Framework
pub mod error;
pub mod intern;
pub mod models;
pub mod pool;
pub mod rate_limit;
pub mod types;

// Alerts & notifications
pub mod alerts;

// Email (Loops.so transactional emails)
pub mod email;

// Platform event bus
pub mod events;

// Auth & identity
pub mod audit;
pub mod auth;
pub mod authorization;
pub mod billing;
pub mod entitlements;
pub mod mfa;
pub mod saml;
pub mod sso;

// Secret slot resolution (shared between Flow and MCP)
pub mod secret_slots;

// Utilities
pub mod domains;
pub mod org_provision;
pub mod pii;
pub mod project_settings;
pub mod prompt_injection;
pub mod query_cache;
pub mod storage;
pub mod utils;

// LLM shared library (used by both Watch and Flow)
pub mod llm;

// PromQL to ClickHouse SQL transpiler
pub mod promql;

// OTel HTTP server metrics middleware (shared by Flow + MCP)
pub mod http_metrics;

// Knowledge base embedding (local ONNX model for vector search)
#[cfg(feature = "embeddings")]
pub mod embeddings;

// State
pub mod app_state;
