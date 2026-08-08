//! Connector factory — creates `Box<dyn Connector>` from source type + JSON config.
//!
//! This module centralises connector instantiation so that `sync_executor`,
//! `job_worker`, and `registry_service` don't duplicate the match logic.

use anyhow::Result;

use crate::warehouse::connectors::{
    Connector,
    // Database connectors
    MySqlConfig, MySqlConnector,
    SqlServerConfig, SqlServerConnector,
    MongoDBConfig, MongoDBConnector,
    PostgresConfig, PostgresConnector,
    SnowflakeConfig, SnowflakeConnector,
    // SaaS / API connectors
    stripe::{StripeConfig, StripeConnector},
    hubspot::{HubSpotConfig, HubSpotConnector},
    salesforce::{SalesforceConfig, SalesforceConnector},
    shopify::{ShopifyConfig, ShopifyConnector},
    notion::{NotionConfig, NotionConnector},
    linear::{LinearConfig, LinearConnector},
    airtable::{AirtableConfig, AirtableConnector},
    jira::{JiraConfig, JiraConnector},
    zendesk::{ZendeskConfig, ZendeskConnector},
    google_ads::{GoogleAdsConfig, GoogleAdsConnector},
    facebook_ads::{FacebookAdsConfig, FacebookAdsConnector},
    intercom::{IntercomConfig, IntercomConnector},
    github::{GitHubConfig, GitHubConnector},
    quickbooks::{QuickBooksConfig, QuickBooksConnector},
    xero::{XeroConfig, XeroConnector},
    mixpanel::{MixpanelConfig, MixpanelConnector},
    woocommerce::{WooCommerceConfig, WooCommerceConnector},
    asana::{AsanaConfig, AsanaConnector},
    posthog::{PostHogConfig, PostHogConnector},
    monday::{MondayConfig, MondayConnector},
};
use crate::warehouse::connectors::oauth::OAuthCredentials;
use crate::warehouse::connectors::blockchain::bitcoin::{BitcoinConfig, BitcoinConnector};
use crate::warehouse::connectors::blockchain::ethereum::{EthereumConfig, EthereumConnector};
use crate::warehouse::connectors::databases::bigquery::{BigQueryConfig, BigQueryConnector};
use crate::warehouse::connectors::databases::clickhouse::{ClickHouseConfig, ClickHouseConnector};
use crate::warehouse::connectors::databases::redshift::{RedshiftConfig, RedshiftConnector};
use crate::warehouse::connectors::databases::sqlite::{SQLiteConfig, SQLiteConnector};
use crate::warehouse::types::SourceType;

/// Create a connector from a `SourceType` and its JSON configuration.
///
/// Returns `Err` for source types that are not yet implemented.
pub async fn create_connector(
    source_type: SourceType,
    config: &serde_json::Value,
) -> Result<Box<dyn Connector>> {
    match source_type {
        // ── Database connectors ──────────────────────────────────────
        SourceType::MySQL => {
            let mysql_config = parse_mysql_config(config)?;
            Ok(Box::new(MySqlConnector::new(mysql_config)))
        }
        SourceType::PostgreSQL => {
            let pg_config = parse_postgres_config(config)?;
            Ok(Box::new(PostgresConnector::new(pg_config)))
        }
        SourceType::SqlServer => {
            let cfg = parse_sqlserver_config(config)?;
            Ok(Box::new(SqlServerConnector::new(cfg)))
        }
        SourceType::MongoDB => {
            let cfg = parse_mongodb_config(config)?;
            Ok(Box::new(MongoDBConnector::new(cfg)))
        }
        SourceType::Snowflake => {
            let cfg = parse_snowflake_config(config)?;
            Ok(Box::new(SnowflakeConnector::new(cfg)))
        }
        SourceType::BigQuery => {
            let cfg = parse_bigquery_config(config)?;
            Ok(Box::new(BigQueryConnector::new(cfg)))
        }
        SourceType::SQLite => {
            let cfg = parse_sqlite_config(config)?;
            Ok(Box::new(SQLiteConnector::new(cfg)))
        }
        SourceType::Redshift => {
            let cfg = parse_redshift_config(config)?;
            Ok(Box::new(RedshiftConnector::new(cfg)))
        }

        // ── SaaS / API connectors ────────────────────────────────────
        SourceType::Stripe => {
            let api_key = config
                .get("api_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Stripe config missing api_key"))?;
            let mut stripe_cfg = StripeConfig::new(api_key);
            if let Some(account_id) = config.get("account_id").and_then(|v| v.as_str()) {
                stripe_cfg = stripe_cfg.with_account_id(account_id);
            }
            Ok(Box::new(StripeConnector::new(stripe_cfg)))
        }

        SourceType::HubSpot => {
            let cfg = parse_hubspot_config(config)?;
            Ok(Box::new(HubSpotConnector::new(cfg)))
        }

        SourceType::Salesforce => {
            let cfg = parse_salesforce_config(config)?;
            Ok(Box::new(SalesforceConnector::new(cfg)))
        }

        SourceType::Shopify => {
            let cfg = parse_shopify_config(config)?;
            Ok(Box::new(ShopifyConnector::new(cfg)))
        }

        SourceType::Notion => {
            let cfg = parse_notion_config(config)?;
            Ok(Box::new(NotionConnector::new(cfg)?))
        }

        SourceType::Linear => {
            let api_key = required_str(config, "api_key", "Linear")?;
            Ok(Box::new(LinearConnector::new(LinearConfig::new(api_key))))
        }

        SourceType::Airtable => {
            let cfg = parse_airtable_config(config)?;
            Ok(Box::new(AirtableConnector::new(cfg)))
        }

        SourceType::Jira => {
            let cfg = parse_jira_config(config)?;
            Ok(Box::new(JiraConnector::new(cfg)))
        }

        SourceType::Zendesk => {
            let cfg = parse_zendesk_config(config)?;
            Ok(Box::new(ZendeskConnector::new(cfg)))
        }

        SourceType::GoogleAds => {
            let cfg = parse_google_ads_config(config)?;
            Ok(Box::new(GoogleAdsConnector::new(cfg)))
        }

        SourceType::FacebookAds => {
            let cfg = parse_facebook_ads_config(config)?;
            Ok(Box::new(FacebookAdsConnector::new(cfg)))
        }

        SourceType::Intercom => {
            let cfg = parse_intercom_config(config)?;
            Ok(Box::new(IntercomConnector::new(cfg)))
        }

        SourceType::GitHub => {
            let cfg = parse_github_config(config)?;
            Ok(Box::new(GitHubConnector::new(cfg)))
        }

        SourceType::QuickBooks => {
            let cfg = parse_quickbooks_config(config)?;
            Ok(Box::new(QuickBooksConnector::new(cfg)))
        }

        SourceType::Xero => {
            let cfg = parse_xero_config(config)?;
            Ok(Box::new(XeroConnector::new(cfg)))
        }

        SourceType::Mixpanel => {
            let cfg = parse_mixpanel_config(config)?;
            Ok(Box::new(MixpanelConnector::new(cfg)))
        }

        SourceType::WooCommerce => {
            let cfg = parse_woocommerce_config(config)?;
            Ok(Box::new(WooCommerceConnector::new(cfg)))
        }

        SourceType::Asana => {
            let cfg = parse_asana_config(config)?;
            Ok(Box::new(AsanaConnector::new(cfg)))
        }

        SourceType::PostHog => {
            let cfg = parse_posthog_config(config)?;
            Ok(Box::new(PostHogConnector::new(cfg)))
        }

        SourceType::Monday => {
            let cfg = parse_monday_config(config)?;
            Ok(Box::new(MondayConnector::new(cfg)))
        }

        SourceType::ClickHouse => {
            let cfg = parse_clickhouse_config(config)?;
            Ok(Box::new(ClickHouseConnector::new(cfg).await))
        }

        // ── Blockchain connectors ─────────────────────────────────────
        SourceType::Bitcoin => {
            let cfg = parse_bitcoin_config(config)?;
            Ok(Box::new(BitcoinConnector::new(cfg)))
        }

        SourceType::Ethereum => {
            let cfg = parse_ethereum_config(config)?;
            Ok(Box::new(EthereumConnector::new(cfg)))
        }

        // ── Sources that don't use sync connectors ───────────────────
        SourceType::ExternalParquet => Err(anyhow::anyhow!(
            "External Parquet sources use cold tier and don't use sync connectors"
        )),

        SourceType::Derived => Err(anyhow::anyhow!(
            "Derived sources are refreshed via the materializer, not sync connectors"
        )),

        // ── Not yet implemented ──────────────────────────────────────
        other => Err(anyhow::anyhow!("{} connector not yet implemented", other)),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Config parsing helpers
// ═══════════════════════════════════════════════════════════════════════════

fn required_str<'a>(config: &'a serde_json::Value, key: &str, ctx: &str) -> Result<&'a str> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("{} config missing '{}'", ctx, key))
}

fn parse_mysql_config(config: &serde_json::Value) -> Result<MySqlConfig> {
    if let Some(conn_str) = config.get("connection_string").and_then(|v| v.as_str()) {
        return Ok(MySqlConfig::new(conn_str.to_string()));
    }
    let host = required_str(config, "host", "MySQL")?;
    let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(3306);
    let database = required_str(config, "database", "MySQL")?;
    let username = required_str(config, "username", "MySQL")?;
    let password = config.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let conn = format!(
        "mysql://{}:{}@{}:{}/{}",
        urlencoding::encode(username),
        urlencoding::encode(password),
        host, port, database
    );
    Ok(MySqlConfig::new(conn))
}

fn parse_postgres_config(config: &serde_json::Value) -> Result<PostgresConfig> {
    if let Some(conn_str) = config.get("connection_string").and_then(|v| v.as_str()) {
        return Ok(PostgresConfig::new(conn_str.to_string()));
    }
    let host = required_str(config, "host", "PostgreSQL")?;
    let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(5432);
    let database = required_str(config, "database", "PostgreSQL")?;
    let username = required_str(config, "username", "PostgreSQL")?;
    let password = config.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let conn = format!(
        "postgres://{}:{}@{}:{}/{}",
        urlencoding::encode(username),
        urlencoding::encode(password),
        host, port, database
    );
    Ok(PostgresConfig::new(conn))
}

fn parse_sqlserver_config(config: &serde_json::Value) -> Result<SqlServerConfig> {
    let host = required_str(config, "host", "SqlServer")?;
    let port = config.get("port").and_then(|v| v.as_u64()).unwrap_or(1433) as u16;
    let database = required_str(config, "database", "SqlServer")?;
    let username = required_str(config, "username", "SqlServer")?;
    let password = config.get("password").and_then(|v| v.as_str()).unwrap_or("");

    let mut cfg = SqlServerConfig::new(
        host.to_string(),
        database.to_string(),
        username.to_string(),
        password.to_string(),
    );
    cfg.port = port;
    cfg.trust_server_certificate = config
        .get("trust_server_certificate")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Ok(cfg)
}

fn parse_mongodb_config(config: &serde_json::Value) -> Result<MongoDBConfig> {
    let conn_str = required_str(config, "connection_string", "MongoDB")?;
    let database = required_str(config, "database", "MongoDB")?;
    Ok(MongoDBConfig::new(conn_str.to_string(), database.to_string()))
}

fn parse_snowflake_config(config: &serde_json::Value) -> Result<SnowflakeConfig> {
    let account = required_str(config, "account", "Snowflake")?;
    let warehouse = required_str(config, "warehouse", "Snowflake")?;
    let database = required_str(config, "database", "Snowflake")?;
    let username = required_str(config, "username", "Snowflake")?;
    let password = required_str(config, "password", "Snowflake")?;

    let mut cfg = SnowflakeConfig::new(account, warehouse, database, username, password);

    if let Some(schema) = config.get("schema").and_then(|v| v.as_str()) {
        cfg = cfg.with_schema(schema);
    }
    if let Some(role) = config.get("role").and_then(|v| v.as_str()) {
        cfg = cfg.with_role(role);
    }
    if let Some(tables) = config.get("tables").and_then(|v| v.as_array()) {
        let names: Vec<String> = tables.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        cfg = cfg.with_tables(names);
    }
    Ok(cfg)
}

fn parse_bigquery_config(config: &serde_json::Value) -> Result<BigQueryConfig> {
    let project_id = required_str(config, "project_id", "BigQuery")?;
    let dataset = required_str(config, "dataset", "BigQuery")?;

    let mut cfg = BigQueryConfig::new(project_id, dataset);

    if let Some(creds_json) = config.get("credentials_json").and_then(|v| v.as_str()) {
        cfg = cfg.with_credentials_json(creds_json);
    } else if let Some(creds_path) = config.get("credentials_path").and_then(|v| v.as_str()) {
        cfg = cfg.with_credentials_path(creds_path);
    }
    if let Some(location) = config.get("location").and_then(|v| v.as_str()) {
        cfg = cfg.with_location(location);
    }
    if let Some(tables) = config.get("tables").and_then(|v| v.as_array()) {
        let names: Vec<String> = tables.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        cfg = cfg.with_tables(names);
    }
    if let Some(max_bytes) = config.get("maximum_bytes_billed").and_then(|v| v.as_i64()) {
        cfg = cfg.with_maximum_bytes_billed(max_bytes);
    }
    Ok(cfg)
}

fn parse_sqlite_config(config: &serde_json::Value) -> Result<SQLiteConfig> {
    let path = required_str(config, "database_path", "SQLite")?;
    let mut cfg = SQLiteConfig::new(path);

    if let Some(tables) = config.get("tables").and_then(|v| v.as_array()) {
        let names: Vec<String> = tables.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        cfg = cfg.with_tables(names);
    }
    if let Some(read_only) = config.get("read_only").and_then(|v| v.as_bool()) {
        cfg = cfg.with_read_only(read_only);
    }
    Ok(cfg)
}

fn parse_redshift_config(config: &serde_json::Value) -> Result<RedshiftConfig> {
    let host = required_str(config, "host", "Redshift")?;
    let database = required_str(config, "database", "Redshift")?;
    let username = required_str(config, "username", "Redshift")?;
    let password = required_str(config, "password", "Redshift")?;
    let mut cfg = RedshiftConfig::new(host, database, username, password);
    if let Some(port) = config.get("port").and_then(|v| v.as_u64()) {
        cfg = cfg.with_port(port as u16);
    }
    if let Some(schema) = config.get("schema").and_then(|v| v.as_str()) {
        cfg = cfg.with_schema(schema);
    }
    if let Some(tables) = config.get("tables").and_then(|v| v.as_array()) {
        let names: Vec<String> = tables.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        cfg = cfg.with_tables(names);
    }
    if let Some(ssl) = config.get("ssl_mode").and_then(|v| v.as_str()) {
        if let Ok(mode) = ssl.parse() {
            cfg = cfg.with_ssl_mode(mode);
        }
    }
    Ok(cfg)
}

fn parse_clickhouse_config(config: &serde_json::Value) -> Result<ClickHouseConfig> {
    use super::databases::clickhouse::ClickHouseProtocol;

    let host = config.get("host").and_then(|v| v.as_str()).unwrap_or("localhost").to_string();
    let database = config.get("database").and_then(|v| v.as_str()).unwrap_or("default").to_string();

    let mut cfg = ClickHouseConfig::new(host, database);

    if let Some(http_port) = config.get("http_port").and_then(|v| v.as_u64()) {
        cfg.http_port = http_port as u16;
    } else if let Some(port) = config.get("port").and_then(|v| v.as_u64()) {
        cfg.http_port = port as u16;
    }
    if let Some(native_port) = config.get("native_port").and_then(|v| v.as_u64()) {
        cfg.native_port = native_port as u16;
    }
    if let Some(proto_str) = config.get("protocol").and_then(|v| v.as_str()) {
        if let Ok(proto) = proto_str.parse::<ClickHouseProtocol>() {
            cfg = cfg.with_protocol(proto);
        }
    }
    if let Some(username) = config.get("username").and_then(|v| v.as_str()) {
        let password = config.get("password").and_then(|v| v.as_str()).unwrap_or("");
        cfg = cfg.with_credentials(username, password);
    }
    if let Some(tables) = config.get("tables").and_then(|v| v.as_array()) {
        let names: Vec<String> = tables
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        cfg = cfg.with_tables(names);
    }
    if let Some(timeout) = config.get("connect_timeout_secs").and_then(|v| v.as_u64()) {
        cfg = cfg.with_connect_timeout(timeout);
    }
    Ok(cfg)
}

fn parse_bitcoin_config(config: &serde_json::Value) -> Result<BitcoinConfig> {
    let rpc_url = required_str(config, "rpc_url", "Bitcoin")?;
    let mut cfg = BitcoinConfig::new(rpc_url);

    if let Some(user) = config.get("rpc_user").and_then(|v| v.as_str()) {
        cfg.rpc_user = Some(user.to_string());
    }
    if let Some(pass) = config.get("rpc_password").and_then(|v| v.as_str()) {
        cfg.rpc_password = Some(pass.to_string());
    }
    if let Some(network) = config.get("network") {
        cfg.network = serde_json::from_value(network.clone())
            .unwrap_or_default();
    }
    if let Some(timeout) = config.get("timeout_secs").and_then(|v| v.as_u64()) {
        cfg.timeout_secs = timeout;
    }
    if let Some(batch) = config.get("batch_size").and_then(|v| v.as_u64()) {
        cfg.batch_size = batch;
    }
    Ok(cfg)
}

fn parse_ethereum_config(config: &serde_json::Value) -> Result<EthereumConfig> {
    // Validate required field before deserializing so we get a clear error.
    required_str(config, "rpc_url", "Ethereum")?;
    serde_json::from_value::<EthereumConfig>(config.clone())
        .map_err(|e| anyhow::anyhow!("Invalid Ethereum config: {}", e))
}

fn parse_hubspot_config(config: &serde_json::Value) -> Result<HubSpotConfig> {
    let oauth = config
        .get("oauth")
        .ok_or_else(|| anyhow::anyhow!("HubSpot config missing 'oauth' credentials"))?;

    let creds: OAuthCredentials = serde_json::from_value(oauth.clone())
        .map_err(|e| anyhow::anyhow!("Invalid HubSpot OAuth config: {}", e))?;

    Ok(HubSpotConfig::new(creds.to_config()))
}

fn parse_shopify_config(config: &serde_json::Value) -> Result<ShopifyConfig> {
    let shop_name = required_str(config, "shop_name", "Shopify")?;
    let api_key = required_str(config, "api_key", "Shopify")?;

    let mut cfg = ShopifyConfig::new(shop_name, api_key);

    if let Some(version) = config.get("api_version").and_then(|v| v.as_str()) {
        cfg = cfg.with_api_version(version);
    }

    Ok(cfg)
}

fn parse_notion_config(config: &serde_json::Value) -> Result<NotionConfig> {
    let api_token = required_str(config, "api_token", "Notion")?;
    Ok(NotionConfig::new(api_token))
}

fn parse_airtable_config(config: &serde_json::Value) -> Result<AirtableConfig> {
    let api_key = required_str(config, "api_key", "Airtable")?;
    let base_id = required_str(config, "base_id", "Airtable")?;
    Ok(AirtableConfig::new(api_key, base_id))
}

fn parse_zendesk_config(config: &serde_json::Value) -> Result<ZendeskConfig> {
    let subdomain = required_str(config, "subdomain", "Zendesk")?.to_string();

    if let Some(oauth_token) = config.get("oauth_token").and_then(|v| v.as_str()) {
        return Ok(ZendeskConfig::with_oauth(subdomain, oauth_token));
    }

    let email = required_str(config, "email", "Zendesk")?;
    let api_token = required_str(config, "api_token", "Zendesk")?;
    Ok(ZendeskConfig::with_api_token(subdomain, email, api_token))
}

fn parse_jira_config(config: &serde_json::Value) -> Result<JiraConfig> {
    let host = required_str(config, "host", "Jira")?.to_string();

    if let Some(pat) = config.get("personal_access_token").and_then(|v| v.as_str()) {
        return Ok(JiraConfig::with_pat(host, pat));
    }

    let email = required_str(config, "email", "Jira")?;
    let api_token = required_str(config, "api_token", "Jira")?;
    Ok(JiraConfig::with_basic_auth(host, email, api_token))
}

fn parse_google_ads_config(config: &serde_json::Value) -> Result<GoogleAdsConfig> {
    let developer_token = required_str(config, "developer_token", "GoogleAds")?.to_string();
    let client_id = required_str(config, "client_id", "GoogleAds")?.to_string();
    let client_secret = required_str(config, "client_secret", "GoogleAds")?.to_string();
    let refresh_token = required_str(config, "refresh_token", "GoogleAds")?.to_string();
    let customer_id = required_str(config, "customer_id", "GoogleAds")?.to_string();

    let mut cfg = GoogleAdsConfig::new(
        developer_token,
        client_id,
        client_secret,
        refresh_token,
        customer_id,
    );

    if let Some(login_id) = config.get("login_customer_id").and_then(|v| v.as_str()) {
        cfg = cfg.with_login_customer_id(login_id);
    }

    Ok(cfg)
}

fn parse_facebook_ads_config(config: &serde_json::Value) -> Result<FacebookAdsConfig> {
    let access_token = required_str(config, "access_token", "FacebookAds")?;
    let ad_account_id = required_str(config, "ad_account_id", "FacebookAds")?;

    let mut cfg = FacebookAdsConfig::new(access_token, ad_account_id);

    if let (Some(app_id), Some(app_secret)) = (
        config.get("app_id").and_then(|v| v.as_str()),
        config.get("app_secret").and_then(|v| v.as_str()),
    ) {
        cfg = cfg.with_app_credentials(app_id, app_secret);
    }

    Ok(cfg)
}

fn parse_intercom_config(config: &serde_json::Value) -> Result<IntercomConfig> {
    let access_token = required_str(config, "access_token", "Intercom")?;
    let mut cfg = IntercomConfig::new(access_token);
    if let Some(region) = config.get("region").and_then(|v| v.as_str()) {
        cfg = cfg.with_region(region);
    }
    Ok(cfg)
}

fn parse_github_config(config: &serde_json::Value) -> Result<GitHubConfig> {
    let access_token = required_str(config, "access_token", "GitHub")?;
    let owner = required_str(config, "owner", "GitHub")?.to_string();
    let mut cfg = GitHubConfig::new(access_token, owner);
    if let Some(repos) = config.get("repos").and_then(|v| v.as_array()) {
        let names: Vec<String> = repos
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        cfg = cfg.with_repos(names);
    }
    if let Some(api_base) = config.get("api_base").and_then(|v| v.as_str()) {
        cfg = cfg.with_api_base(api_base);
    }
    Ok(cfg)
}

fn parse_salesforce_config(config: &serde_json::Value) -> Result<SalesforceConfig> {
    let oauth = config
        .get("oauth")
        .ok_or_else(|| anyhow::anyhow!("Salesforce config missing 'oauth' credentials"))?;

    let creds: OAuthCredentials = serde_json::from_value(oauth.clone())
        .map_err(|e| anyhow::anyhow!("Invalid Salesforce OAuth config: {}", e))?;

    let instance_url = config
        .get("instance_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Salesforce config missing 'instance_url'"))?;

    let mut sf_config = SalesforceConfig::new(creds.to_config(), instance_url);

    if let Some(version) = config.get("api_version").and_then(|v| v.as_str()) {
        sf_config = sf_config.with_api_version(version);
    }

    Ok(sf_config)
}

fn parse_quickbooks_config(config: &serde_json::Value) -> Result<QuickBooksConfig> {
    let oauth = config
        .get("oauth")
        .ok_or_else(|| anyhow::anyhow!("QuickBooks config missing 'oauth' credentials"))?;

    let creds: OAuthCredentials = serde_json::from_value(oauth.clone())
        .map_err(|e| anyhow::anyhow!("Invalid QuickBooks OAuth config: {}", e))?;

    let realm_id = required_str(config, "realm_id", "QuickBooks")?;
    let sandbox = config
        .get("sandbox")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Ok(QuickBooksConfig::new(creds.to_config(), realm_id).with_sandbox(sandbox))
}

fn parse_xero_config(config: &serde_json::Value) -> Result<XeroConfig> {
    let access_token = required_str(config, "access_token", "Xero")?;
    let tenant_id = required_str(config, "tenant_id", "Xero")?.to_string();
    let mut cfg = XeroConfig::new(access_token, tenant_id);
    if let Some(api_base) = config.get("api_base").and_then(|v| v.as_str()) {
        cfg = cfg.with_api_base(api_base);
    }
    Ok(cfg)
}

fn parse_mixpanel_config(config: &serde_json::Value) -> Result<MixpanelConfig> {
    let api_secret = required_str(config, "api_secret", "Mixpanel")?;
    let project_id = required_str(config, "project_id", "Mixpanel")?.to_string();
    let mut cfg = MixpanelConfig::new(api_secret, project_id);
    if let Some(region) = config.get("region").and_then(|v| v.as_str()) {
        cfg = cfg.with_region(region);
    }
    Ok(cfg)
}

fn parse_woocommerce_config(config: &serde_json::Value) -> Result<WooCommerceConfig> {
    let consumer_key = required_str(config, "consumer_key", "WooCommerce")?;
    let consumer_secret = required_str(config, "consumer_secret", "WooCommerce")?;
    let store_url = required_str(config, "store_url", "WooCommerce")?.to_string();
    Ok(WooCommerceConfig::new(consumer_key, consumer_secret, store_url))
}

fn parse_asana_config(config: &serde_json::Value) -> Result<AsanaConfig> {
    let token = required_str(config, "personal_access_token", "Asana")?;
    let workspace_gid = required_str(config, "workspace_gid", "Asana")?.to_string();
    Ok(AsanaConfig::new(token, workspace_gid))
}

fn parse_posthog_config(config: &serde_json::Value) -> Result<PostHogConfig> {
    let api_key = required_str(config, "personal_api_key", "PostHog")?;
    let project_id = required_str(config, "project_id", "PostHog")?.to_string();
    let mut cfg = PostHogConfig::new(api_key, project_id);
    if let Some(api_base) = config.get("api_base").and_then(|v| v.as_str()) {
        cfg = cfg.with_api_base(api_base);
    }
    Ok(cfg)
}

fn parse_monday_config(config: &serde_json::Value) -> Result<MondayConfig> {
    let token = required_str(config, "api_token", "Monday")?;
    let mut cfg = MondayConfig::new(token);
    if let Some(board_ids) = config.get("board_ids").and_then(|v| v.as_array()) {
        let ids: Vec<u64> = board_ids
            .iter()
            .filter_map(|v| v.as_u64())
            .collect();
        cfg = cfg.with_board_ids(ids);
    }
    Ok(cfg)
}
