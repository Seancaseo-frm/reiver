//! Postgres Wire Protocol adapter for Pond.
//!
//! Enables BI tools (Tableau, Metabase, DBeaver, psql, etc.) to connect
//! directly to Pond using the standard PostgreSQL wire protocol. Queries
//! are routed through the same federated query engine as the HTTP API.
//!
//! ## Authentication
//!
//! Connections authenticate using the Postgres `user` field as a project
//! API key. The password is accepted but ignored. On success, the validated
//! `project_id` is stored on the connection for all subsequent queries.
//!
//! ## Usage
//!
//! ```text
//! psql -h localhost -p 5433 -U sk_live_abc123def456
//! ```

mod auth;
pub mod catalog;
pub mod dialect;
pub mod handler;
pub mod server;
pub mod session;
pub mod types;

#[cfg(test)]
mod bi_compat_tests;

pub use server::start_pgwire_server;
