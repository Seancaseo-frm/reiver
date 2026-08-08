//! Reiver Herd -- A2A Agent Registry and Message Hub
//!
//! This crate provides the Herd product:
//! - A2A v1.0 agent registry with discovery and cross-org access grants
//! - JSON-RPC 2.0 message routing between registered agents
//! - Enterprise pipeline (PII scrubbing, injection detection)
//! - Push notification delivery via Redpanda with retry

pub use reiver_core::{audit, clickhouse_db, config, db, error, kafka, models, types, utils};

pub mod a2a;
pub mod access_cache;
pub mod api;
pub mod app_state;
pub mod auth;
pub mod pipeline;
pub mod routing_cache;
pub mod telemetry;
pub mod verification;
pub mod worker;
