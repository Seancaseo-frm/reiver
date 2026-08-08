use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

pub type DbPool = PgPool;

pub async fn ensure_database_exists(database_url: &str) -> anyhow::Result<()> {
    // Parse the database URL to extract the database name.
    // Use sqlx's own parser to avoid WHATWG URL-spec quirks with the
    // `postgresql://` non-special scheme (the `url` crate can mangle
    // the authority when doing string replacement on opaque-path URLs).
    let opts: sqlx::postgres::PgConnectOptions = database_url.parse()?;
    let db_name = match opts.get_database() {
        Some(name) if !name.is_empty() => name.to_owned(),
        _ => return Ok(()),
    };

    let postgres_opts = opts.clone().database("postgres");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(postgres_opts)
        .await;

    let pool = match pool {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                "Could not connect to 'postgres' database to check if '{}' exists \
                 (this is expected with managed operators like CloudNativePG): {}",
                db_name,
                e
            );
            return Ok(());
        }
    };

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&db_name)
            .fetch_one(&pool)
            .await?;

    if !exists {
        tracing::info!("Creating database: {}", db_name);
        if !db_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            anyhow::bail!("Invalid database name: {}", db_name);
        }
        sqlx::query(&format!("CREATE DATABASE {}", db_name))
            .execute(&pool)
            .await?;
    }

    pool.close().await;
    Ok(())
}

pub async fn create_pool(database_url: &str) -> anyhow::Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await?;

    Ok(pool)
}
