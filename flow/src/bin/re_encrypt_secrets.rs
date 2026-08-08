//! CLI tool to re-encrypt all secrets in the database after a key rotation.
//!
//! Usage:
//!   re-encrypt-secrets [--dry-run]
//!
//! Environment variables:
//!   DATABASE_URL      - PostgreSQL connection string
//!   ENCRYPTION_KEY    - New (primary) encryption key (base64-encoded 32 bytes)
//!   ENCRYPTION_KEY_OLD - Old key(s) for fallback decryption (comma-separated)

use anyhow::{Context, Result};
use clap::Parser;
use reiver_core::crypto::RotatingSecretEncryptor;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "re-encrypt-secrets")]
#[command(about = "Re-encrypt all secrets using the current primary encryption key")]
struct Cli {
    /// Report what would be re-encrypted without writing changes
    #[arg(long)]
    dry_run: bool,
}

struct ReEncryptStats {
    checked: u64,
    re_encrypted: u64,
    errors: u64,
    skipped_null: u64,
}

impl ReEncryptStats {
    fn new() -> Self {
        Self {
            checked: 0,
            re_encrypted: 0,
            errors: 0,
            skipped_null: 0,
        }
    }

    fn merge(&mut self, other: &ReEncryptStats) {
        self.checked += other.checked;
        self.re_encrypted += other.re_encrypted;
        self.errors += other.errors;
        self.skipped_null += other.skipped_null;
    }
}

/// A table column that holds encrypted data.
struct EncryptedColumn {
    table: &'static str,
    id_column: &'static str,
    encrypted_column: &'static str,
    /// Optional WHERE clause to filter rows (e.g., only look at filled slots)
    where_clause: Option<&'static str>,
}

const ENCRYPTED_COLUMNS: &[EncryptedColumn] = &[
    EncryptedColumn {
        table: "sso_connections",
        id_column: "id",
        encrypted_column: "client_secret_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "sso_connections",
        id_column: "id",
        encrypted_column: "sp_private_key_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "sso_connections",
        id_column: "id",
        encrypted_column: "okta_api_token_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "deploy_keys",
        id_column: "id",
        encrypted_column: "private_key_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "mfa_factors",
        id_column: "id",
        encrypted_column: "secret_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "notification_channels",
        id_column: "id",
        encrypted_column: "api_token_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "notification_channels",
        id_column: "id",
        encrypted_column: "client_secret_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "warehouse_sources",
        id_column: "id",
        encrypted_column: "secret_access_key_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "warehouse_sources",
        id_column: "id",
        encrypted_column: "password_encrypted",
        where_clause: None,
    },
    EncryptedColumn {
        table: "project_settings",
        id_column: "id",
        encrypted_column: "value",
        where_clause: Some("key LIKE 'gateway_%_api_key'"),
    },
    EncryptedColumn {
        table: "secret_slots",
        id_column: "id",
        encrypted_column: "encrypted_value",
        where_clause: Some("status = 'filled'"),
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let start = Instant::now();

    println!("=== Secret Re-Encryption Tool ===");
    if cli.dry_run {
        println!("MODE: dry-run (no writes)");
    } else {
        println!("MODE: live (will update database)");
    }
    println!();

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

    let encryptor = RotatingSecretEncryptor::from_env()
        .map_err(|e| anyhow::anyhow!("Failed to initialize encryptor: {}", e))?;

    if encryptor.fallback_key_count() == 0 {
        println!("WARNING: No ENCRYPTION_KEY_OLD set. Nothing to re-encrypt unless data was");
        println!("         encrypted with a different key than ENCRYPTION_KEY.");
        println!();
    } else {
        println!(
            "Encryptor initialized with {} fallback key(s)",
            encryptor.fallback_key_count()
        );
        println!();
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to connect to database")?;

    println!("Connected to database.");
    println!();

    let mut total_stats = ReEncryptStats::new();

    for col in ENCRYPTED_COLUMNS {
        let stats = process_column(&pool, &encryptor, col, cli.dry_run).await?;
        total_stats.merge(&stats);
    }

    let elapsed = start.elapsed();
    println!();
    println!("=== Summary ===");
    println!("  Rows checked:      {}", total_stats.checked);
    println!("  Rows re-encrypted: {}", total_stats.re_encrypted);
    println!("  Rows skipped (NULL): {}", total_stats.skipped_null);
    println!("  Errors:            {}", total_stats.errors);
    println!("  Elapsed:           {:.2}s", elapsed.as_secs_f64());

    if total_stats.errors > 0 {
        eprintln!();
        eprintln!("WARNING: {} rows failed to re-encrypt. Review errors above.", total_stats.errors);
        std::process::exit(1);
    }

    if !cli.dry_run && total_stats.re_encrypted > 0 {
        println!();
        println!("Re-encryption complete. Run with --dry-run to verify 0 rows remain.");
    }

    Ok(())
}

async fn process_column(
    pool: &PgPool,
    encryptor: &RotatingSecretEncryptor,
    col: &EncryptedColumn,
    dry_run: bool,
) -> Result<ReEncryptStats> {
    let mut stats = ReEncryptStats::new();

    let where_clause = match col.where_clause {
        Some(w) => format!("WHERE {} IS NOT NULL AND {}", col.encrypted_column, w),
        None => format!("WHERE {} IS NOT NULL", col.encrypted_column),
    };

    let query = format!(
        "SELECT {id}::text AS id, {col} AS encrypted_value FROM {table} {where_clause}",
        id = col.id_column,
        col = col.encrypted_column,
        table = col.table,
        where_clause = where_clause,
    );

    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to query {}.{}", col.table, col.encrypted_column))?;

    let total = rows.len();
    let mut re_encrypted = 0u64;
    let mut errors = 0u64;

    for row in &rows {
        let id: String = row.get("id");
        let encrypted_value: String = row.get("encrypted_value");

        stats.checked += 1;

        if !encryptor.needs_re_encryption(&encrypted_value) {
            continue;
        }

        match encryptor.re_encrypt(&encrypted_value) {
            Ok(new_value) => {
                if !dry_run {
                    let update_query = format!(
                        "UPDATE {} SET {} = $1 WHERE {}::text = $2",
                        col.table, col.encrypted_column, col.id_column,
                    );
                    if let Err(e) = sqlx::query(&update_query)
                        .bind(&new_value)
                        .bind(&id)
                        .execute(pool)
                        .await
                    {
                        eprintln!(
                            "  ERROR updating {}.{} id={}: {}",
                            col.table, col.encrypted_column, id, e
                        );
                        errors += 1;
                        continue;
                    }
                }
                re_encrypted += 1;
            }
            Err(e) => {
                eprintln!(
                    "  ERROR re-encrypting {}.{} id={}: {}",
                    col.table, col.encrypted_column, id, e
                );
                errors += 1;
            }
        }
    }

    if re_encrypted > 0 || errors > 0 {
        let action = if dry_run { "would re-encrypt" } else { "re-encrypted" };
        println!(
            "  {}.{}: checked {}, {} {}, {} errors",
            col.table, col.encrypted_column, total, action, re_encrypted, errors
        );
    } else if total > 0 {
        println!(
            "  {}.{}: checked {} — all up to date",
            col.table, col.encrypted_column, total
        );
    }

    stats.re_encrypted = re_encrypted;
    stats.errors = errors;
    Ok(stats)
}
