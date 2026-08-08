//! PgWire TCP server for Pond.
//!
//! Listens on a configurable port (default 5433) and dispatches incoming
//! PostgreSQL wire protocol connections to the handler pipeline:
//!
//! 1. `ProjectKeyStartupHandler` – authenticates via project API key
//! 2. `PondQueryHandler` – routes SQL through the warehouse query engine
//!
//! TLS is supported via `tokio-rustls`. Set `PGWIRE_TLS_CERT` and
//! `PGWIRE_TLS_KEY` environment variables to enable encrypted connections.
//! When unset, the server accepts plain TCP (suitable for local development).

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::PgWireServerHandlers;
use pgwire::tokio::process_socket;

use super::auth::ProjectKeyStartupHandler;
use super::handler::PondQueryHandler;
use crate::app_state::PondState;

const MAX_PGWIRE_CONNECTIONS: usize = 512;

/// Factory that creates pgwire handler instances for each connection.
///
/// Implements `PgWireServerHandlers` so it can be passed to `process_socket`.
/// All handlers share the same `PondState` via `Arc`.
struct PondPgWireHandlers {
    startup_handler: Arc<ProjectKeyStartupHandler>,
    query_handler: Arc<PondQueryHandler>,
}

impl PgWireServerHandlers for PondPgWireHandlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query_handler.clone()
    }
}

/// Build a TLS acceptor from PEM certificate and key files.
///
/// Returns `None` if the `PGWIRE_TLS_CERT` or `PGWIRE_TLS_KEY` environment
/// variables are not set. Returns an error if the files exist but cannot be
/// loaded or parsed.
fn setup_tls() -> anyhow::Result<Option<pgwire::tokio::TlsAcceptor>> {
    let cert_path = match std::env::var("PGWIRE_TLS_CERT") {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    let key_path = match std::env::var("PGWIRE_TLS_KEY") {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };

    let certs: Vec<rustls_pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(File::open(&cert_path)?))
            .collect::<Result<Vec<_>, _>>()?;

    let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(File::open(&key_path)?))
        .next()
        .ok_or_else(|| anyhow::anyhow!("No PKCS8 private key found in {}", key_path))??;

    let mut config = pgwire::tokio::tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, rustls_pki_types::PrivateKeyDer::Pkcs8(key))
        .map_err(|e| anyhow::anyhow!("TLS config error: {}", e))?;

    // PostgreSQL ALPN protocol identifier
    config.alpn_protocols = vec![b"postgresql".to_vec()];

    tracing::info!(
        cert = %cert_path,
        key = %key_path,
        "PgWire TLS enabled"
    );

    Ok(Some(pgwire::tokio::TlsAcceptor::from(Arc::new(config))))
}

/// Start the pgwire server alongside the HTTP API.
///
/// Listens on `PGWIRE_LISTEN_ADDR` (default `0.0.0.0:5433`) and accepts
/// Postgres wire protocol connections. Each connection is authenticated
/// using a project API key and can execute SQL queries through Pond's
/// federated query engine.
///
/// If `PGWIRE_TLS_CERT` and `PGWIRE_TLS_KEY` are set, connections are
/// encrypted with TLS. Otherwise, plain TCP is used.
///
/// This function runs until the listener encounters a fatal error.
/// It is designed to be spawned alongside the Axum HTTP server via `tokio::select!`.
pub async fn start_pgwire_server(state: Arc<PondState>) -> anyhow::Result<()> {
    let addr = std::env::var("PGWIRE_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:5433".to_string());

    let tls_acceptor = setup_tls()?;

    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(
        addr = %addr,
        tls = tls_acceptor.is_some(),
        "PgWire server listening"
    );

    let handlers = Arc::new(PondPgWireHandlers {
        startup_handler: Arc::new(ProjectKeyStartupHandler::new(state.clone())),
        query_handler: Arc::new(PondQueryHandler::new(state)),
    });

    let conn_semaphore = Arc::new(Semaphore::new(MAX_PGWIRE_CONNECTIONS));
    let mut consecutive_errors = 0u32;

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                consecutive_errors = 0;
                let handlers = handlers.clone();
                let tls = tls_acceptor.clone();
                let permit = match conn_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!(
                            peer_addr = %peer_addr,
                            max = MAX_PGWIRE_CONNECTIONS,
                            "PgWire connection limit reached, dropping connection"
                        );
                        drop(socket);
                        continue;
                    }
                };
                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, tls, handlers).await {
                        tracing::debug!(
                            peer_addr = %peer_addr,
                            error = %e,
                            "PgWire connection error"
                        );
                    }
                    drop(permit);
                });
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!(error = %e, consecutive_errors, "Failed to accept PgWire connection");
                let backoff_ms = std::cmp::min(100 * 2u64.pow(consecutive_errors.min(7)), 10_000);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
}
