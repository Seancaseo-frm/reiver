//! SSO Maintenance Worker
//!
//! Background worker for SSO-related maintenance tasks:
//! - Periodic SAML certificate expiry checks
//! - Session cleanup (expired/revoked)
//!
//! Runs every hour to check certificate status and clean up stale sessions.

use anyhow::Result;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info};

use crate::db::DbPool;

/// Start the SSO maintenance worker
///
/// Runs every hour and performs:
/// - Certificate expiry checks (logs warnings for expiring certificates)
/// - Session cleanup (removes expired/revoked sessions)
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `shutdown_rx` - Shutdown signal receiver for graceful shutdown
pub async fn start_sso_worker(
    db_pool: Arc<DbPool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    info!("Starting SSO maintenance worker (runs every hour)");

    // Run every hour
    let mut interval = time::interval(time::Duration::from_secs(3600));

    let handle = tokio::spawn(async move {
        // Run immediately on startup, then every hour
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("SSO maintenance worker tick");

                    // Check certificate expiry
                    if let Err(e) = crate::api::sso::check_certificate_expiry(&db_pool).await {
                        error!("Certificate expiry check failed: {}", e);
                    }

                    // Clean up expired/revoked sessions
                    if let Err(e) = cleanup_expired_sessions(&db_pool).await {
                        error!("Session cleanup failed: {}", e);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("SSO maintenance worker received shutdown signal, stopping gracefully");
                        break;
                    }
                }
            }
        }
        info!("SSO maintenance worker stopped");
    });

    Ok(handle)
}

/// Clean up expired and revoked SSO sessions older than 7 days
///
/// This removes sessions that are:
/// - Expired (expires_at < NOW())
/// - Revoked more than 7 days ago
///
/// We keep recently revoked sessions for audit purposes.
async fn cleanup_expired_sessions(db: &DbPool) -> Result<()> {
    // Delete sessions that expired more than 7 days ago
    let expired_result = sqlx::query(
        r#"
        DELETE FROM sso_sessions
        WHERE expires_at < NOW() - INTERVAL '7 days'
        "#,
    )
    .execute(db)
    .await?;

    // Delete sessions that were revoked more than 7 days ago
    let revoked_result = sqlx::query(
        r#"
        DELETE FROM sso_sessions
        WHERE revoked_at IS NOT NULL 
          AND revoked_at < NOW() - INTERVAL '7 days'
        "#,
    )
    .execute(db)
    .await?;

    let total_deleted = expired_result.rows_affected() + revoked_result.rows_affected();

    if total_deleted > 0 {
        info!(
            "SSO session cleanup: deleted {} expired, {} revoked sessions",
            expired_result.rows_affected(),
            revoked_result.rows_affected()
        );
    }

    Ok(())
}
