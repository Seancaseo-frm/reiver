//! Connector catalog — UI metadata and config field schemas for every implemented connector.
//!
//! The frontend fetches this catalog via the `/warehouse/connector-types` API endpoint
//! and uses it to dynamically render the source type picker and configuration form.
//! This keeps the single source of truth in the backend, next to the connector code.

use serde::Serialize;
use crate::warehouse::types::SourceType;

#[derive(Debug, Clone)]
pub struct ConnectorMeta {
    pub source_type: SourceType,
    pub name: String,
    pub description: String,
    pub icon: String,
    pub category: ConnectorCategory,
    pub config_fields: Vec<FieldDef>,
}

impl ConnectorMeta {
    /// Globally-synced sources (blockchain) don't need user-provided credentials.
    /// Users only pick a display name to enable them.
    pub fn is_global(&self) -> bool {
        self.source_type.is_blockchain()
    }
}

impl serde::Serialize for ConnectorMeta {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ConnectorMeta", 7)?;
        s.serialize_field("source_type", &self.source_type)?;
        s.serialize_field("name", &self.name)?;
        s.serialize_field("description", &self.description)?;
        s.serialize_field("icon", &self.icon)?;
        s.serialize_field("category", &self.category)?;
        s.serialize_field("is_global", &self.is_global())?;
        s.serialize_field("config_fields", &self.config_fields)?;
        Ok(s.end()?)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorCategory {
    Database,
    #[serde(rename = "saas")]
    SaaS,
    Blockchain,
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldDef {
    pub key: String,
    pub label: String,
    pub field_type: FieldType,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<SelectOption>>,
    pub width: FieldWidth,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Password,
    Number,
    Select,
    Textarea,
    Toggle,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldWidth {
    Full,
    Half,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

// ─── Builder helpers ────────────────────────────────────────────────────────

fn text(key: &str, label: &str) -> FieldDef {
    FieldDef {
        key: key.into(),
        label: label.into(),
        field_type: FieldType::Text,
        required: true,
        placeholder: None,
        help_text: None,
        default_value: None,
        options: None,
        width: FieldWidth::Full,
    }
}

fn password(key: &str, label: &str) -> FieldDef {
    FieldDef {
        field_type: FieldType::Password,
        ..text(key, label)
    }
}

fn number(key: &str, label: &str) -> FieldDef {
    FieldDef {
        field_type: FieldType::Number,
        ..text(key, label)
    }
}

fn textarea(key: &str, label: &str) -> FieldDef {
    FieldDef {
        field_type: FieldType::Textarea,
        ..text(key, label)
    }
}

fn select(key: &str, label: &str, options: Vec<(&str, &str)>) -> FieldDef {
    FieldDef {
        field_type: FieldType::Select,
        options: Some(
            options
                .into_iter()
                .map(|(v, l)| SelectOption {
                    value: v.into(),
                    label: l.into(),
                })
                .collect(),
        ),
        ..text(key, label)
    }
}

fn toggle(key: &str, label: &str) -> FieldDef {
    FieldDef {
        field_type: FieldType::Toggle,
        required: false,
        ..text(key, label)
    }
}

impl FieldDef {
    fn half(mut self) -> Self {
        self.width = FieldWidth::Half;
        self
    }
    fn optional(mut self) -> Self {
        self.required = false;
        self
    }
    fn placeholder(mut self, p: &str) -> Self {
        self.placeholder = Some(p.into());
        self
    }
    fn help(mut self, h: &str) -> Self {
        self.help_text = Some(h.into());
        self
    }
    fn default_str(mut self, v: &str) -> Self {
        self.default_value = Some(serde_json::Value::String(v.into()));
        self
    }
    fn default_num(mut self, v: u64) -> Self {
        self.default_value = Some(serde_json::Value::Number(v.into()));
        self
    }
    fn default_bool(mut self, v: bool) -> Self {
        self.default_value = Some(serde_json::Value::Bool(v));
        self
    }
}

// ─── Catalog ────────────────────────────────────────────────────────────────

/// All source types that have a working connector in `factory.rs`.
/// Used by tests to ensure the catalog stays in sync with the factory.
pub const IMPLEMENTED_SOURCE_TYPES: &[SourceType] = &[
    SourceType::PostgreSQL,
    SourceType::MySQL,
    SourceType::SqlServer,
    SourceType::MongoDB,
    SourceType::Snowflake,
    SourceType::BigQuery,
    SourceType::SQLite,
    SourceType::Redshift,
    SourceType::ClickHouse,
    SourceType::Stripe,
    SourceType::HubSpot,
    SourceType::Salesforce,
    SourceType::Shopify,
    SourceType::Notion,
    SourceType::Linear,
    SourceType::Airtable,
    SourceType::Jira,
    SourceType::Zendesk,
    SourceType::GoogleAds,
    SourceType::FacebookAds,
    SourceType::Intercom,
    SourceType::GitHub,
    SourceType::QuickBooks,
    SourceType::Xero,
    SourceType::Mixpanel,
    SourceType::WooCommerce,
    SourceType::Asana,
    SourceType::PostHog,
    SourceType::Monday,
    SourceType::Bitcoin,
    SourceType::Ethereum,
];

/// Return UI metadata and config-field schemas for every implemented connector.
pub fn connector_catalog() -> Vec<ConnectorMeta> {
    vec![
        // ── Databases ───────────────────────────────────────────────────
        ConnectorMeta {
            source_type: SourceType::PostgreSQL,
            name: "PostgreSQL".into(),
            description: "Open-source relational database".into(),
            icon: "\u{1F418}".into(), // 🐘
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("host", "Host").half().placeholder("localhost"),
                number("port", "Port").half().default_num(5432),
                text("database", "Database").placeholder("mydb"),
                text("username", "Username").half(),
                password("password", "Password").half().optional(),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::MySQL,
            name: "MySQL".into(),
            description: "Popular relational database".into(),
            icon: "\u{1F42C}".into(), // 🐬
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("host", "Host").half().placeholder("localhost"),
                number("port", "Port").half().default_num(3306),
                text("database", "Database").placeholder("mydb"),
                text("username", "Username").half(),
                password("password", "Password").half().optional(),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::SqlServer,
            name: "SQL Server".into(),
            description: "Microsoft SQL Server".into(),
            icon: "\u{1F537}".into(), // 🔷
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("host", "Host").half(),
                number("port", "Port").half().default_num(1433).optional(),
                text("database", "Database"),
                text("username", "Username").half(),
                password("password", "Password").half().optional(),
                toggle("trust_server_certificate", "Trust Server Certificate")
                    .help("Skip TLS certificate verification"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::MongoDB,
            name: "MongoDB".into(),
            description: "Document database".into(),
            icon: "\u{1F343}".into(), // 🍃
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("connection_string", "Connection String")
                    .placeholder("mongodb://user:pass@host:27017"),
                text("database", "Database"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Snowflake,
            name: "Snowflake".into(),
            description: "Cloud data warehouse".into(),
            icon: "\u{2744}\u{FE0F}".into(), // ❄️
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("account", "Account Identifier").half().placeholder("xy12345.us-east-1.aws"),
                text("warehouse", "Warehouse").half().placeholder("COMPUTE_WH"),
                text("database", "Database").half().placeholder("ANALYTICS_DB"),
                text("schema", "Schema").half().optional().placeholder("PUBLIC"),
                text("username", "Username").half(),
                password("password", "Password").half(),
                text("role", "Role").optional().placeholder("ANALYST"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::BigQuery,
            name: "BigQuery".into(),
            description: "Google Cloud data warehouse".into(),
            icon: "\u{2601}\u{FE0F}".into(), // ☁️
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("project_id", "Project ID"),
                text("dataset", "Dataset"),
                textarea("credentials_json", "Service Account JSON")
                    .optional()
                    .placeholder("Paste your service account JSON here..."),
                text("location", "Location").optional().placeholder("US"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::SQLite,
            name: "SQLite".into(),
            description: "Embedded database".into(),
            icon: "\u{1F4E6}".into(), // 📦
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("database_path", "Database Path").placeholder("/path/to/database.sqlite"),
                toggle("read_only", "Read Only").default_bool(true),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Redshift,
            name: "Redshift".into(),
            description: "Amazon Redshift data warehouse".into(),
            icon: "\u{1F536}".into(), // 🔶
            category: ConnectorCategory::Database,
            config_fields: vec![
                text("host", "Cluster Endpoint").half()
                    .placeholder("my-cluster.xxxx.us-east-1.redshift.amazonaws.com"),
                number("port", "Port").half().optional().default_num(5439),
                text("database", "Database").half().placeholder("dev"),
                text("schema", "Schema").half().optional().placeholder("public"),
                text("username", "Username").half(),
                password("password", "Password").half(),
                select("ssl_mode", "SSL Mode", vec![
                    ("disable", "Disable"),
                    ("prefer", "Prefer"),
                    ("require", "Require"),
                ]).optional().default_str("prefer"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::ClickHouse,
            name: "ClickHouse".into(),
            description: "Columnar analytical database".into(),
            icon: "\u{26A1}".into(), // ⚡
            category: ConnectorCategory::Database,
            config_fields: vec![
                select("protocol", "Protocol", vec![
                    ("native", "Native TCP (recommended)"),
                    ("http", "HTTP"),
                ]).default_str("native")
                    .help("Native TCP is faster. Falls back to HTTP if connection fails."),
                text("host", "Host").half().placeholder("localhost").default_str("localhost"),
                number("port", "Port").half().default_num(9000)
                    .help("Native default: 9000, HTTP default: 8123"),
                text("database", "Database").default_str("default"),
                text("username", "Username").half().default_str("default"),
                password("password", "Password").half().optional()
                    .help("Optional for local instances"),
            ],
        },

        // ── SaaS / API ─────────────────────────────────────────────────
        ConnectorMeta {
            source_type: SourceType::Stripe,
            name: "Stripe".into(),
            description: "Payment processing platform".into(),
            icon: "\u{1F4B3}".into(), // 💳
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_key", "API Key (Secret)").placeholder("sk_live_..."),
                text("account_id", "Connected Account ID").optional()
                    .placeholder("acct_...")
                    .help("For Stripe Connect — leave empty for your own account"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::HubSpot,
            name: "HubSpot".into(),
            description: "CRM and marketing automation".into(),
            icon: "\u{1F4E7}".into(), // 📧
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                textarea("oauth", "OAuth Credentials (JSON)")
                    .placeholder(r#"{"access_token":"...","refresh_token":"...","client_id":"...","client_secret":"..."}"#)
                    .help("Paste the full OAuth credentials JSON object"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Salesforce,
            name: "Salesforce".into(),
            description: "CRM platform".into(),
            icon: "\u{2601}\u{FE0F}".into(), // ☁️
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                textarea("oauth", "OAuth Credentials (JSON)")
                    .placeholder(r#"{"access_token":"...","refresh_token":"...","client_id":"...","client_secret":"..."}"#),
                text("instance_url", "Instance URL").placeholder("https://yourorg.my.salesforce.com"),
                text("api_version", "API Version").optional().placeholder("v59.0"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Shopify,
            name: "Shopify".into(),
            description: "E-commerce platform".into(),
            icon: "\u{1F6D2}".into(), // 🛒
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                text("shop_name", "Shop Name").placeholder("your-store"),
                password("api_key", "Admin API Access Token").placeholder("shpat_..."),
                text("api_version", "API Version").optional().placeholder("2024-01"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Notion,
            name: "Notion".into(),
            description: "Workspace and knowledge base".into(),
            icon: "\u{1F4DD}".into(), // 📝
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_token", "Integration Token").placeholder("secret_..."),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Linear,
            name: "Linear".into(),
            description: "Issue tracking".into(),
            icon: "\u{1F3AF}".into(), // 🎯
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_key", "API Key").placeholder("lin_api_..."),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Airtable,
            name: "Airtable".into(),
            description: "Spreadsheet-database hybrid".into(),
            icon: "\u{1F4CA}".into(), // 📊
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_key", "Personal Access Token").placeholder("pat..."),
                text("base_id", "Base ID").placeholder("app..."),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Jira,
            name: "Jira".into(),
            description: "Project management".into(),
            icon: "\u{1F4CB}".into(), // 📋
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                text("host", "Host").placeholder("yourorg.atlassian.net"),
                text("email", "Email").half().optional()
                    .help("Required for basic auth; omit if using PAT"),
                password("api_token", "API Token").half().optional()
                    .help("Required for basic auth"),
                password("personal_access_token", "Personal Access Token").optional()
                    .help("Alternative to email + API token"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Zendesk,
            name: "Zendesk".into(),
            description: "Customer support platform".into(),
            icon: "\u{1F4DE}".into(), // 📞
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                text("subdomain", "Subdomain").placeholder("yourcompany"),
                text("email", "Email").half().optional()
                    .help("Required for API token auth"),
                password("api_token", "API Token").half().optional()
                    .help("Required for API token auth"),
                password("oauth_token", "OAuth Token").optional()
                    .help("Alternative to email + API token"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::GoogleAds,
            name: "Google Ads".into(),
            description: "Advertising platform".into(),
            icon: "\u{1F4E2}".into(), // 📢
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("developer_token", "Developer Token"),
                text("client_id", "Client ID").half(),
                password("client_secret", "Client Secret").half(),
                password("refresh_token", "Refresh Token"),
                text("customer_id", "Customer ID").placeholder("123-456-7890"),
                text("login_customer_id", "Login Customer ID").optional()
                    .help("MCC account ID, if applicable"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::FacebookAds,
            name: "Facebook Ads".into(),
            description: "Meta advertising platform".into(),
            icon: "\u{1F4F1}".into(), // 📱
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("access_token", "Access Token"),
                text("ad_account_id", "Ad Account ID").placeholder("act_123456"),
                text("app_id", "App ID").half().optional(),
                password("app_secret", "App Secret").half().optional(),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Intercom,
            name: "Intercom".into(),
            description: "Customer messaging platform".into(),
            icon: "\u{1F4AC}".into(), // 💬
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("access_token", "Access Token"),
                select("region", "Region", vec![
                    ("us", "US"),
                    ("eu", "EU"),
                    ("au", "Australia"),
                ]).optional().default_str("us"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::GitHub,
            name: "GitHub".into(),
            description: "Code hosting and collaboration".into(),
            icon: "\u{1F4BB}".into(), // 💻
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("access_token", "Personal Access Token").placeholder("ghp_..."),
                text("owner", "Owner / Organization"),
                text("api_base", "API Base URL").optional()
                    .placeholder("https://api.github.com")
                    .help("Override for GitHub Enterprise"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::QuickBooks,
            name: "QuickBooks".into(),
            description: "Accounting software".into(),
            icon: "\u{1F4B0}".into(), // 💰
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                textarea("oauth", "OAuth Credentials (JSON)")
                    .placeholder(r#"{"access_token":"...","refresh_token":"...","client_id":"...","client_secret":"..."}"#),
                text("realm_id", "Realm ID (Company ID)"),
                toggle("sandbox", "Sandbox Mode").default_bool(false),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Xero,
            name: "Xero".into(),
            description: "Accounting platform".into(),
            icon: "\u{1F4B2}".into(), // 💲
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("access_token", "Access Token"),
                text("tenant_id", "Tenant ID"),
                text("api_base", "API Base URL").optional()
                    .placeholder("https://api.xero.com"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Mixpanel,
            name: "Mixpanel".into(),
            description: "Product analytics".into(),
            icon: "\u{1F4C8}".into(), // 📈
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_secret", "API Secret"),
                text("project_id", "Project ID"),
                select("region", "Region", vec![
                    ("us", "US"),
                    ("eu", "EU"),
                ]).optional().default_str("us"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::WooCommerce,
            name: "WooCommerce".into(),
            description: "WordPress e-commerce".into(),
            icon: "\u{1F6CD}\u{FE0F}".into(), // 🛍️
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                text("store_url", "Store URL").placeholder("https://yourstore.com"),
                password("consumer_key", "Consumer Key").half(),
                password("consumer_secret", "Consumer Secret").half(),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Asana,
            name: "Asana".into(),
            description: "Project management".into(),
            icon: "\u{2705}".into(), // ✅
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("personal_access_token", "Personal Access Token"),
                text("workspace_gid", "Workspace GID"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::PostHog,
            name: "PostHog".into(),
            description: "Open-source product analytics".into(),
            icon: "\u{1F994}".into(), // 🦔
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("personal_api_key", "Personal API Key"),
                text("project_id", "Project ID"),
                text("api_base", "API Base URL").optional()
                    .placeholder("https://app.posthog.com")
                    .help("Override for self-hosted PostHog"),
            ],
        },
        ConnectorMeta {
            source_type: SourceType::Monday,
            name: "Monday.com".into(),
            description: "Work management platform".into(),
            icon: "\u{1F4C5}".into(), // 📅
            category: ConnectorCategory::SaaS,
            config_fields: vec![
                password("api_token", "API Token"),
            ],
        },

        // ── Blockchain (global sources — no user config needed) ────────
        ConnectorMeta {
            source_type: SourceType::Bitcoin,
            name: "Bitcoin".into(),
            description: "Bitcoin blockchain data — blocks, transactions, inputs, outputs".into(),
            icon: "\u{20BF}".into(), // ₿
            category: ConnectorCategory::Blockchain,
            config_fields: vec![],
        },
        ConnectorMeta {
            source_type: SourceType::Ethereum,
            name: "Ethereum".into(),
            description: "Ethereum blockchain data — blocks, transactions, event logs".into(),
            icon: "\u{039E}".into(), // Ξ
            category: ConnectorCategory::Blockchain,
            config_fields: vec![],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn catalog_covers_all_implemented_source_types() {
        let catalog = connector_catalog();
        let catalog_types: HashSet<SourceType> = catalog.iter().map(|m| m.source_type).collect();

        for st in IMPLEMENTED_SOURCE_TYPES {
            assert!(
                catalog_types.contains(st),
                "SourceType::{:?} is in IMPLEMENTED_SOURCE_TYPES but missing from connector_catalog()",
                st
            );
        }
    }

    #[test]
    fn no_duplicate_source_types_in_catalog() {
        let catalog = connector_catalog();
        let mut seen = HashSet::new();
        for meta in &catalog {
            assert!(
                seen.insert(meta.source_type),
                "Duplicate source_type {:?} in connector_catalog()",
                meta.source_type
            );
        }
    }

    #[test]
    fn catalog_length_matches_implemented_count() {
        let catalog = connector_catalog();
        assert_eq!(
            catalog.len(),
            IMPLEMENTED_SOURCE_TYPES.len(),
            "connector_catalog() returned {} entries but IMPLEMENTED_SOURCE_TYPES has {}",
            catalog.len(),
            IMPLEMENTED_SOURCE_TYPES.len()
        );
    }

    #[test]
    fn every_non_global_connector_has_at_least_one_field() {
        for meta in connector_catalog() {
            if meta.is_global() {
                assert!(
                    meta.config_fields.is_empty(),
                    "{:?} is global but has config_fields (global sources need no user config)",
                    meta.source_type
                );
            } else {
                assert!(
                    !meta.config_fields.is_empty(),
                    "{:?} has no config_fields",
                    meta.source_type
                );
            }
        }
    }

    #[test]
    fn required_field_keys_match_factory() {
        let catalog = connector_catalog();

        let expected: Vec<(SourceType, &[&str])> = vec![
            (SourceType::PostgreSQL, &["host", "database", "username"]),
            (SourceType::MySQL, &["host", "database", "username"]),
            (SourceType::SqlServer, &["host", "database", "username"]),
            (SourceType::MongoDB, &["connection_string", "database"]),
            (SourceType::Snowflake, &["account", "warehouse", "database", "username", "password"]),
            (SourceType::BigQuery, &["project_id", "dataset"]),
            (SourceType::SQLite, &["database_path"]),
            (SourceType::Redshift, &["host", "database", "username", "password"]),
            (SourceType::ClickHouse, &[]),
            (SourceType::Stripe, &["api_key"]),
            (SourceType::HubSpot, &["oauth"]),
            (SourceType::Salesforce, &["oauth", "instance_url"]),
            (SourceType::Shopify, &["shop_name", "api_key"]),
            (SourceType::Notion, &["api_token"]),
            (SourceType::Linear, &["api_key"]),
            (SourceType::Airtable, &["api_key", "base_id"]),
            (SourceType::Jira, &["host"]),
            (SourceType::Zendesk, &["subdomain"]),
            (SourceType::GoogleAds, &["developer_token", "client_id", "client_secret", "refresh_token", "customer_id"]),
            (SourceType::FacebookAds, &["access_token", "ad_account_id"]),
            (SourceType::Intercom, &["access_token"]),
            (SourceType::GitHub, &["access_token", "owner"]),
            (SourceType::QuickBooks, &["oauth", "realm_id"]),
            (SourceType::Xero, &["access_token", "tenant_id"]),
            (SourceType::Mixpanel, &["api_secret", "project_id"]),
            (SourceType::WooCommerce, &["consumer_key", "consumer_secret", "store_url"]),
            (SourceType::Asana, &["personal_access_token", "workspace_gid"]),
            (SourceType::PostHog, &["personal_api_key", "project_id"]),
            (SourceType::Monday, &["api_token"]),
            // Bitcoin and Ethereum are global sources — no user config fields
        ];

        for (source_type, required_keys) in expected {
            let meta = catalog
                .iter()
                .find(|m| m.source_type == source_type)
                .unwrap_or_else(|| panic!("Missing catalog entry for {:?}", source_type));

            let catalog_required: HashSet<&str> = meta
                .config_fields
                .iter()
                .filter(|f| f.required)
                .map(|f| f.key.as_str())
                .collect();

            for key in required_keys {
                assert!(
                    catalog_required.contains(key),
                    "{:?} catalog is missing required field '{}' (expected by factory)",
                    source_type,
                    key
                );
            }
        }
    }

    #[test]
    fn catalog_serializes_to_json() {
        let catalog = connector_catalog();
        let json = serde_json::to_string(&catalog);
        assert!(json.is_ok(), "catalog failed to serialize: {:?}", json.err());

        let roundtrip: Result<Vec<serde_json::Value>, _> =
            serde_json::from_str(&json.unwrap());
        assert!(
            roundtrip.is_ok(),
            "catalog JSON failed to deserialize: {:?}",
            roundtrip.err()
        );
    }
}
