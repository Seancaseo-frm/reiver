//! Notion connector for the data warehouse.
//!
//! Syncs Notion workspace data (pages, databases, users, and dynamic database
//! tables) to the warehouse. Uses the `notion-client` crate with bearer token
//! authentication. The differentiating feature is dynamic schema inference from
//! Notion database properties, enabling each user-defined Notion database to
//! appear as its own table with a proper Arrow schema.

use super::{Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo};
use crate::warehouse::query::predicate_pushdown::Predicate;
use crate::warehouse::types::{ColumnSchema, ColumnType, SourceType, TableSchema};
use super::builders::ColumnBuilders;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crate::crypto::SecretString;
use notion_client::endpoints::Client as NotionClient;
use notion_client::endpoints::search::title::request::{
    SearchByTitleRequest, Filter as SearchFilter, FilterProperty, FilterValue,
    Sort as SearchSort, SortDirection as SearchSortDirection,
    Timestamp as SearchTimestamp,
};
use notion_client::endpoints::search::title::response::PageOrDatabase;
use notion_client::endpoints::databases::query::request::{
    QueryDatabaseRequest, Filter as DbFilter, FilterType, PropertyCondition,
    RichTextCondition, NumberCondition, DateCondition, CheckBoxCondition,
    TimestampCondition, Timestamp as DbTimestamp,
    Sort as DbSort, SortDirection as DbSortDirection,
};
use notion_client::objects::database::{Database, DatabaseProperty};
use notion_client::objects::page::{Page, PageProperty};
use notion_client::objects::property::DateOrDateTime;
use notion_client::objects::parent::Parent;
use notion_client::objects::rich_text::RichText;
use notion_client::objects::user::UserType;
use notion_client::NotionClientError;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

const BATCH_THRESHOLD: usize = 500;
const MAX_TOTAL_ROWS: usize = 1_000_000;
const RATE_LIMIT_DELAY_MS: u64 = 350;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

const STATIC_TABLES: &[&str] = &["pages", "databases", "users"];
const DB_TABLE_PREFIX: &str = "db_";

fn is_rate_limited(err: &NotionClientError) -> bool {
    matches!(err, NotionClientError::InvalidStatusCode { error } if error.status == 429)
}

async fn call_with_retry<F, Fut, T>(f: F) -> Result<T, NotionClientError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, NotionClientError>>,
{
    let mut attempts = 0u32;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if is_rate_limited(&e) && attempts < MAX_RETRIES {
                    attempts += 1;
                    let delay = INITIAL_RETRY_DELAY_MS * 2u64.pow(attempts - 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

static PAGES_SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
    columns: vec![
        ColumnSchema::new("id", ColumnType::String, false)
            .with_description("Notion page ID"),
        ColumnSchema::new("title", ColumnType::String, true)
            .with_description("Page title extracted from Title property"),
        ColumnSchema::new("created_time", ColumnType::Timestamp, true)
            .with_description("Page creation timestamp")
            .with_timezone("UTC"),
        ColumnSchema::new("last_edited_time", ColumnType::Timestamp, true)
            .with_description("Page last edit timestamp")
            .with_timezone("UTC"),
        ColumnSchema::new("created_by_id", ColumnType::String, true)
            .with_description("ID of user who created the page"),
        ColumnSchema::new("last_edited_by_id", ColumnType::String, true)
            .with_description("ID of user who last edited the page"),
        ColumnSchema::new("archived", ColumnType::Boolean, true)
            .with_description("Whether the page is archived"),
        ColumnSchema::new("url", ColumnType::String, true)
            .with_description("Notion URL for the page"),
        ColumnSchema::new("parent_type", ColumnType::String, true)
            .with_description("Type of parent (database, page, workspace, block)"),
        ColumnSchema::new("parent_id", ColumnType::String, true)
            .with_description("ID of the parent object"),
    ],
});

static DATABASES_SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
    columns: vec![
        ColumnSchema::new("id", ColumnType::String, false)
            .with_description("Notion database ID"),
        ColumnSchema::new("title", ColumnType::String, true)
            .with_description("Database title"),
        ColumnSchema::new("description", ColumnType::String, true)
            .with_description("Database description"),
        ColumnSchema::new("created_time", ColumnType::Timestamp, true)
            .with_description("Database creation timestamp")
            .with_timezone("UTC"),
        ColumnSchema::new("last_edited_time", ColumnType::Timestamp, true)
            .with_description("Database last edit timestamp")
            .with_timezone("UTC"),
        ColumnSchema::new("archived", ColumnType::Boolean, true)
            .with_description("Whether the database is archived"),
        ColumnSchema::new("url", ColumnType::String, true)
            .with_description("Notion URL for the database"),
        ColumnSchema::new("is_inline", ColumnType::Boolean, true)
            .with_description("Whether the database is inline within a page"),
        ColumnSchema::new("parent_type", ColumnType::String, true)
            .with_description("Type of parent (database, page, workspace, block)"),
        ColumnSchema::new("parent_id", ColumnType::String, true)
            .with_description("ID of the parent object"),
    ],
});

static USERS_SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
    columns: vec![
        ColumnSchema::new("id", ColumnType::String, false)
            .with_description("Notion user ID"),
        ColumnSchema::new("name", ColumnType::String, true)
            .with_description("User display name"),
        ColumnSchema::new("email", ColumnType::String, true)
            .with_description("User email (people only)"),
        ColumnSchema::new("user_type", ColumnType::String, true)
            .with_description("User type: person or bot"),
        ColumnSchema::new("avatar_url", ColumnType::String, true)
            .with_description("URL of user avatar image"),
    ],
});

/// Notion connector configuration.
#[derive(Clone)]
pub struct NotionConfig {
    pub api_token: SecretString,
}

impl std::fmt::Debug for NotionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotionConfig")
            .field("api_token", &"***REDACTED***")
            .finish()
    }
}

impl NotionConfig {
    pub fn new(api_token: impl Into<String>) -> Self {
        Self {
            api_token: SecretString::new(api_token.into()),
        }
    }
}

/// Notion workspace data source connector.
pub struct NotionConnector {
    #[allow(dead_code)]
    config: NotionConfig,
    client: NotionClient,
}


impl NotionConnector {
    pub fn new(config: NotionConfig) -> ConnectorResult<Self> {
        let client = NotionClient::new(config.api_token.expose().to_string(), None)
            .map_err(|e| ConnectorError::Config(format!("Failed to create Notion client: {}", e)))?;
        Ok(Self { config, client })
    }

    fn get_static_schema(table: &str) -> Option<&'static TableSchema> {
        match table {
            "pages" => Some(&PAGES_SCHEMA),
            "databases" => Some(&DATABASES_SCHEMA),
            "users" => Some(&USERS_SCHEMA),
            _ => None,
        }
    }

    // ── Rich text helpers ─────────────────────────────────────────────

    fn rich_text_to_plain(texts: &[RichText]) -> String {
        texts
            .iter()
            .filter_map(|rt| match rt {
                RichText::Text { plain_text, .. } => plain_text.clone(),
                RichText::Mention { plain_text, .. } => Some(plain_text.clone()),
                RichText::Equation { plain_text, .. } => Some(plain_text.clone()),
                RichText::None => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }

    // ── Parent helpers ────────────────────────────────────────────────

    fn parent_type_str(parent: &Parent) -> &'static str {
        match parent {
            Parent::DatabaseId { .. } => "database",
            Parent::PageId { .. } => "page",
            Parent::Workspace { .. } => "workspace",
            Parent::BlockId { .. } => "block",
            Parent::None => "none",
            Parent::DataSourceId { .. } => "data_source",
        }
    }

    fn parent_id_str(parent: &Parent) -> String {
        match parent {
            Parent::DatabaseId { database_id } => database_id.clone(),
            Parent::PageId { page_id } => page_id.clone(),
            Parent::Workspace { .. } => "workspace".to_string(),
            Parent::BlockId { block_id } => block_id.clone(),
            Parent::None => String::new(),
            Parent::DataSourceId { data_source_id } => data_source_id.clone(),
        }
    }

    // ── Property name sanitization ────────────────────────────────────

    fn sanitize_property_name(name: &str) -> String {
        let s: String = name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();
        let s = s.trim_matches('_').to_lowercase();
        if s.is_empty() {
            "unnamed_property".to_string()
        } else if s.chars().next().map_or(false, |c| c.is_ascii_digit()) {
            format!("col_{}", s)
        } else {
            s
        }
    }

    // ── Dynamic schema inference ──────────────────────────────────────

    fn database_property_to_column_type(prop: &DatabaseProperty) -> Option<ColumnType> {
        match prop {
            DatabaseProperty::Title { .. }
            | DatabaseProperty::RichText { .. }
            | DatabaseProperty::Url { .. }
            | DatabaseProperty::Email { .. }
            | DatabaseProperty::PhoneNumber { .. } => Some(ColumnType::String),

            DatabaseProperty::Number { .. } => Some(ColumnType::Float64),

            DatabaseProperty::Checkbox { .. } => Some(ColumnType::Boolean),

            DatabaseProperty::Date { .. }
            | DatabaseProperty::CreatedTime { .. }
            | DatabaseProperty::LastEditedTime { .. } => Some(ColumnType::Timestamp),

            DatabaseProperty::Select { .. }
            | DatabaseProperty::Status { .. }
            | DatabaseProperty::MultiSelect { .. }
            | DatabaseProperty::People { .. }
            | DatabaseProperty::Relation { .. }
            | DatabaseProperty::Formula { .. }
            | DatabaseProperty::Rollup { .. }
            | DatabaseProperty::Files { .. } => Some(ColumnType::String),

            DatabaseProperty::CreatedBy { .. }
            | DatabaseProperty::LastEditedBy { .. } => Some(ColumnType::String),

            DatabaseProperty::Button { .. } => None,
        }
    }

    fn build_schema_and_prop_map(db: &Database) -> (TableSchema, HashMap<String, String>) {
        let mut columns = vec![
            ColumnSchema::new("id", ColumnType::String, false)
                .with_description("Notion page ID within this database"),
            ColumnSchema::new("created_time", ColumnType::Timestamp, true)
                .with_description("Row creation timestamp")
                .with_timezone("UTC"),
            ColumnSchema::new("last_edited_time", ColumnType::Timestamp, true)
                .with_description("Row last edit timestamp")
                .with_timezone("UTC"),
            ColumnSchema::new("url", ColumnType::String, true)
                .with_description("Notion URL for this row"),
            ColumnSchema::new("archived", ColumnType::Boolean, true)
                .with_description("Whether the row is archived"),
        ];

        let mut prop_map = HashMap::new();
        let mut sorted_props: Vec<_> = db.properties.iter().collect();
        sorted_props.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut seen_names: HashMap<String, usize> = HashMap::new();
        for (original_name, prop) in &sorted_props {
            if let Some(col_type) = Self::database_property_to_column_type(prop) {
                let mut sanitized = Self::sanitize_property_name(original_name);

                let base_columns = ["id", "created_time", "last_edited_time", "url", "archived"];
                if base_columns.contains(&sanitized.as_str()) {
                    sanitized = format!("prop_{}", sanitized);
                }

                let count = seen_names.entry(sanitized.clone()).or_insert(0);
                let final_name = if *count > 0 {
                    format!("{}_{}", sanitized, count)
                } else {
                    sanitized.clone()
                };
                *count += 1;

                let mut col = ColumnSchema::new(&final_name, col_type, true)
                    .with_description(*original_name);
                if col_type == ColumnType::Timestamp {
                    col = col.with_timezone("UTC");
                }
                columns.push(col);
                prop_map.insert(final_name, original_name.to_string());
            }
        }

        (TableSchema { columns }, prop_map)
    }

    fn database_table_id(db_id: &str) -> String {
        format!("{}{}", DB_TABLE_PREFIX, db_id.replace('-', ""))
    }

    fn extract_database_id(table_name: &str) -> Option<&str> {
        table_name.strip_prefix(DB_TABLE_PREFIX)
    }

    // ── PageProperty value extraction ─────────────────────────────────

    fn extract_page_property_string(prop: &PageProperty) -> Option<String> {
        match prop {
            PageProperty::Title { title, .. } => {
                let text = Self::rich_text_to_plain(title);
                if text.is_empty() { None } else { Some(text) }
            }
            PageProperty::RichText { rich_text, .. } => {
                let text = Self::rich_text_to_plain(rich_text);
                if text.is_empty() { None } else { Some(text) }
            }
            PageProperty::Number { number, .. } => {
                number.as_ref().map(|n| n.to_string())
            }
            PageProperty::Select { select, .. } => {
                select.as_ref().and_then(|s| s.name.clone())
            }
            PageProperty::Status { status, .. } => {
                status.as_ref().and_then(|s| s.name.clone())
            }
            PageProperty::MultiSelect { multi_select, .. } => {
                let names: Vec<String> = multi_select.iter().filter_map(|s| s.name.clone()).collect();
                if names.is_empty() { None } else { Some(names.join(", ")) }
            }
            PageProperty::Checkbox { checkbox, .. } => {
                Some(checkbox.to_string())
            }
            PageProperty::Url { url, .. } => url.clone(),
            PageProperty::Email { email, .. } => email.clone(),
            PageProperty::PhoneNumber { phone_number, .. } => phone_number.clone(),
            PageProperty::Date { date, .. } => {
                date.as_ref().and_then(|d| d.start.as_ref().map(|s| Self::date_or_datetime_to_string(s)))
            }
            PageProperty::People { people, .. } => {
                let ids: Vec<String> = people.iter().map(|u| u.id.clone()).collect();
                if ids.is_empty() { None } else { Some(ids.join(", ")) }
            }
            PageProperty::Relation { relation, .. } => {
                let ids: Vec<String> = relation.iter().map(|r| r.id.clone()).collect();
                if ids.is_empty() { None } else { Some(ids.join(", ")) }
            }
            PageProperty::Files { files, .. } => {
                let urls: Vec<String> = files.iter().map(|f| f.name.clone()).collect();
                if urls.is_empty() { None } else { Some(urls.join(", ")) }
            }
            PageProperty::Formula { formula, .. } => {
                formula.as_ref().map(|f| format!("{:?}", f))
            }
            PageProperty::Rollup { rollup, .. } => {
                rollup.as_ref().map(|r| format!("{:?}", r))
            }
            PageProperty::CreatedBy { created_by, .. } => Some(created_by.id.clone()),
            PageProperty::LastEditedBy { last_edited_by, .. } => Some(last_edited_by.id.clone()),
            PageProperty::CreatedTime { created_time, .. } => {
                Some(created_time.to_rfc3339())
            }
            PageProperty::LastEditedTime { last_edited_time, .. } => {
                last_edited_time.as_ref().map(|dt| dt.to_rfc3339())
            }
            PageProperty::UniqueID { unique_id, .. } => {
                unique_id.as_ref().map(|u| format!(
                    "{}{}",
                    u.prefix.as_deref().map(|p| format!("{}-", p)).unwrap_or_default(),
                    u.number.as_ref().map(|n| n.to_string()).unwrap_or_default(),
                ))
            }
            PageProperty::Verification { .. } | PageProperty::Button { .. } => None,
        }
    }

    fn extract_page_property_f64(prop: &PageProperty) -> Option<f64> {
        match prop {
            PageProperty::Number { number, .. } => {
                number.as_ref().and_then(|n| n.as_f64())
            }
            _ => {
                Self::extract_page_property_string(prop)
                    .and_then(|s| s.parse().ok())
            }
        }
    }

    fn extract_page_property_bool(prop: &PageProperty) -> Option<bool> {
        match prop {
            PageProperty::Checkbox { checkbox, .. } => Some(*checkbox),
            _ => None,
        }
    }

    fn extract_page_property_timestamp(prop: &PageProperty) -> Option<i64> {
        match prop {
            PageProperty::Date { date, .. } => {
                date.as_ref().and_then(|d| {
                    d.start.as_ref().map(|s| Self::date_or_datetime_to_micros(s))
                })
            }
            PageProperty::CreatedTime { created_time, .. } => {
                Some(created_time.timestamp_micros())
            }
            PageProperty::LastEditedTime { last_edited_time, .. } => {
                last_edited_time.as_ref().map(|dt| dt.timestamp_micros())
            }
            _ => {
                Self::extract_page_property_string(prop)
                    .and_then(|s| Self::parse_notion_timestamp(&s))
            }
        }
    }

    fn date_or_datetime_to_string(dt: &DateOrDateTime) -> String {
        match dt {
            DateOrDateTime::DateTime(d) => d.to_rfc3339(),
            DateOrDateTime::Date(d) => d.to_string(),
        }
    }

    fn date_or_datetime_to_micros(dt: &DateOrDateTime) -> i64 {
        match dt {
            DateOrDateTime::DateTime(d) => d.timestamp_micros(),
            DateOrDateTime::Date(d) => d.and_hms_opt(0, 0, 0)
                .map(|ndt| ndt.and_utc().timestamp_micros())
                .unwrap_or(0),
        }
    }

    fn parse_notion_timestamp(value: &str) -> Option<i64> {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_micros());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%SZ") {
            return Some(dt.and_utc().timestamp_micros());
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            return d.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc().timestamp_micros());
        }
        None
    }

    fn apply_projection(schema: &TableSchema, projection: &[String]) -> TableSchema {
        if projection.is_empty() {
            return schema.clone();
        }
        TableSchema {
            columns: schema
                .columns
                .iter()
                .filter(|c| c.name == "id" || projection.iter().any(|p| p == &c.name))
                .cloned()
                .collect(),
        }
    }

    fn to_arrow_schema(schema: &TableSchema) -> Schema {
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow_type(), col.nullable))
            .collect();
        Schema::new(fields)
    }

    // ── Arrow columnar builders ──────────────────────────────────────

    // ── Page append (for static "pages" table) ───────────────────────


    fn append_page_metadata(page: &Page, schema: &TableSchema, builders: &mut ColumnBuilders) {
        let title = page.properties.values().find_map(|p| {
            if let PageProperty::Title { title, .. } = p {
                let text = Self::rich_text_to_plain(title);
                if text.is_empty() { None } else { Some(text) }
            } else {
                None
            }
        });

        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(Some(&page.id)),
                "title" => builders.builder(i).append_string(title.as_deref()),
                "created_time" => builders.builder(i).append_timestamp(Some(page.created_time.timestamp_micros())),
                "last_edited_time" => builders.builder(i).append_timestamp(Some(page.last_edited_time.timestamp_micros())),
                "created_by_id" => builders.builder(i).append_string(Some(&page.created_by.id)),
                "last_edited_by_id" => builders.builder(i).append_string(Some(&page.last_edited_by.id)),
                "archived" => builders.builder(i).append_bool(Some(page.in_trash)),
                "url" => builders.builder(i).append_string(Some(&page.url)),
                "parent_type" => builders.builder(i).append_string(Some(Self::parent_type_str(&page.parent))),
                "parent_id" => {
                    let pid = Self::parent_id_str(&page.parent);
                    builders.builder(i).append_string(Some(&pid));
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    // ── Database append (for static "databases" table) ─────────────────

    fn append_database_metadata(db: &Database, schema: &TableSchema, builders: &mut ColumnBuilders) {
        let title = Self::rich_text_to_plain(&db.title);
        let description = Self::rich_text_to_plain(&db.description);

        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(Some(db.id.as_deref().unwrap_or(""))),
                "title" => builders.builder(i).append_string(if title.is_empty() { None } else { Some(&title) }),
                "description" => builders.builder(i).append_string(if description.is_empty() { None } else { Some(&description) }),
                "created_time" => builders.builder(i).append_timestamp(Some(db.created_time.timestamp_micros())),
                "last_edited_time" => builders.builder(i).append_timestamp(Some(db.last_edited_time.timestamp_micros())),
                "archived" => builders.builder(i).append_bool(Some(db.in_trash)),
                "url" => builders.builder(i).append_string(Some(&db.url)),
                "is_inline" => builders.builder(i).append_bool(Some(db.is_inline)),
                "parent_type" => builders.builder(i).append_string(Some(Self::parent_type_str(&db.parent))),
                "parent_id" => {
                    let pid = Self::parent_id_str(&db.parent);
                    builders.builder(i).append_string(Some(&pid));
                }
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    // ── User append ──────────────────────────────────────────────────

    fn append_user(user: &notion_client::objects::user::User, schema: &TableSchema, builders: &mut ColumnBuilders) {
        let (user_type, email) = match &user.user_type {
            Some(UserType::Person { person }) => ("person", Some(person.email.as_str())),
            Some(UserType::Bot { .. }) => ("bot", None),
            None => ("unknown", None),
        };

        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(Some(&user.id)),
                "name" => builders.builder(i).append_string(user.name.as_deref()),
                "email" => builders.builder(i).append_string(email),
                "user_type" => builders.builder(i).append_string(Some(user_type)),
                "avatar_url" => builders.builder(i).append_string(user.avatar_url.as_deref()),
                _ => builders.builder(i).append_null(),
            }
        }
        builders.row_complete();
    }

    // ── Database row append (for dynamic db_<id> tables) ───────────────

    fn append_database_row(
        page: &Page,
        schema: &TableSchema,
        prop_name_map: &HashMap<String, String>,
        builders: &mut ColumnBuilders,
    ) {
        for (i, col) in schema.columns.iter().enumerate() {
            match col.name.as_str() {
                "id" => builders.builder(i).append_string(Some(&page.id)),
                "created_time" => builders.builder(i).append_timestamp(Some(page.created_time.timestamp_micros())),
                "last_edited_time" => builders.builder(i).append_timestamp(Some(page.last_edited_time.timestamp_micros())),
                "url" => builders.builder(i).append_string(Some(&page.url)),
                "archived" => builders.builder(i).append_bool(Some(page.in_trash)),
                _ => {
                    let original_name = prop_name_map.get(&col.name)
                        .map(|s| s.as_str())
                        .or(col.description.as_deref())
                        .unwrap_or(&col.name);

                    if let Some(prop) = page.properties.get(original_name) {
                        match col.data_type {
                            ColumnType::Float64 => {
                                builders.builder(i).append_f64(Self::extract_page_property_f64(prop));
                            }
                            ColumnType::Boolean => {
                                builders.builder(i).append_bool(Self::extract_page_property_bool(prop));
                            }
                            ColumnType::Timestamp => {
                                builders.builder(i).append_timestamp(Self::extract_page_property_timestamp(prop));
                            }
                            _ => {
                                let val = Self::extract_page_property_string(prop);
                                builders.builder(i).append_string(val.as_deref());
                            }
                        }
                    } else {
                        builders.builder(i).append_null();
                    }
                }
            }
        }
        builders.row_complete();
    }

    // ── Predicate pushdown for database queries ───────────────────────

    fn filter_to_filter_types(filter: DbFilter) -> Vec<FilterType> {
        match filter {
            DbFilter::Value { filter_type } => vec![filter_type],
            DbFilter::And { and } => and,
            DbFilter::Or { or } => or,
        }
    }

    fn build_database_filter(
        predicates: &[Predicate],
        prop_name_map: &HashMap<String, String>,
        schema: &TableSchema,
    ) -> Option<DbFilter> {
        let filters: Vec<DbFilter> = predicates
            .iter()
            .filter_map(|p| Self::predicate_to_notion_filter(p, prop_name_map, schema))
            .collect();

        match filters.len() {
            0 => None,
            1 => Some(filters.into_iter().next().unwrap()),
            _ => {
                let filter_types: Vec<FilterType> = filters.into_iter()
                    .flat_map(Self::filter_to_filter_types)
                    .collect();
                Some(DbFilter::And { and: filter_types })
            }
        }
    }

    fn predicate_to_notion_filter(
        predicate: &Predicate,
        prop_name_map: &HashMap<String, String>,
        schema: &TableSchema,
    ) -> Option<DbFilter> {
        match predicate {
            Predicate::Equals { column, value } => {
                let original = prop_name_map.get(column.as_str())?;
                let col_schema = schema.columns.iter().find(|c| c.name == column.as_str())?;

                let condition = match col_schema.data_type {
                    ColumnType::String => PropertyCondition::RichText(
                        RichTextCondition::Equals(value.to_string()),
                    ),
                    ColumnType::Float64 => {
                        let n: f64 = value.parse().ok()?;
                        let num = serde_json::Number::from_f64(n)?;
                        PropertyCondition::Number(NumberCondition::Equals(num))
                    }
                    ColumnType::Boolean => {
                        let b: bool = value.parse().ok()?;
                        PropertyCondition::Checkbox(CheckBoxCondition::Equals(b))
                    }
                    _ => PropertyCondition::RichText(
                        RichTextCondition::Equals(value.to_string()),
                    ),
                };

                Some(DbFilter::Value {
                    filter_type: FilterType::Property {
                        property: original.clone(),
                        condition,
                    },
                })
            }
            Predicate::Contains { column, substring } => {
                let original = prop_name_map.get(column.as_str())?;
                Some(DbFilter::Value {
                    filter_type: FilterType::Property {
                        property: original.clone(),
                        condition: PropertyCondition::RichText(
                            RichTextCondition::Contains(substring.to_string()),
                        ),
                    },
                })
            }
            Predicate::GreaterThan { column, value, inclusive } => {
                let original = prop_name_map.get(column.as_str())?;
                let col_schema = schema.columns.iter().find(|c| c.name == column.as_str())?;

                if col_schema.data_type == ColumnType::Float64 {
                    let n: f64 = value.parse().ok()?;
                    let num = serde_json::Number::from_f64(n)?;
                    let cond = if *inclusive {
                        NumberCondition::GreaterThanOrEqualTo(num)
                    } else {
                        NumberCondition::GreaterThan(num)
                    };
                    Some(DbFilter::Value {
                        filter_type: FilterType::Property {
                            property: original.clone(),
                            condition: PropertyCondition::Number(cond),
                        },
                    })
                } else if col_schema.data_type == ColumnType::Timestamp {
                    let dt = chrono::DateTime::parse_from_rfc3339(value.as_str()).ok()?;
                    let utc_dt = dt.with_timezone(&chrono::Utc);
                    let cond = if *inclusive {
                        DateCondition::OnOrAfter(utc_dt)
                    } else {
                        DateCondition::After(utc_dt)
                    };
                    Some(DbFilter::Value {
                        filter_type: FilterType::Property {
                            property: original.clone(),
                            condition: PropertyCondition::Date(cond),
                        },
                    })
                } else {
                    None
                }
            }
            Predicate::LessThan { column, value, inclusive } => {
                let original = prop_name_map.get(column.as_str())?;
                let col_schema = schema.columns.iter().find(|c| c.name == column.as_str())?;

                if col_schema.data_type == ColumnType::Float64 {
                    let n: f64 = value.parse().ok()?;
                    let num = serde_json::Number::from_f64(n)?;
                    let cond = if *inclusive {
                        NumberCondition::LessThanOrEqualTo(num)
                    } else {
                        NumberCondition::LessThan(num)
                    };
                    Some(DbFilter::Value {
                        filter_type: FilterType::Property {
                            property: original.clone(),
                            condition: PropertyCondition::Number(cond),
                        },
                    })
                } else if col_schema.data_type == ColumnType::Timestamp {
                    let dt = chrono::DateTime::parse_from_rfc3339(value.as_str()).ok()?;
                    let utc_dt = dt.with_timezone(&chrono::Utc);
                    let cond = if *inclusive {
                        DateCondition::OnOrBefore(utc_dt)
                    } else {
                        DateCondition::Before(utc_dt)
                    };
                    Some(DbFilter::Value {
                        filter_type: FilterType::Property {
                            property: original.clone(),
                            condition: PropertyCondition::Date(cond),
                        },
                    })
                } else {
                    None
                }
            }
            Predicate::And(inner) => {
                let filter_types: Vec<FilterType> = inner
                    .iter()
                    .filter_map(|p| Self::predicate_to_notion_filter(p, prop_name_map, schema))
                    .flat_map(Self::filter_to_filter_types)
                    .collect();
                if filter_types.is_empty() { None } else { Some(DbFilter::And { and: filter_types }) }
            }
            Predicate::Or(inner) => {
                let filter_types: Vec<FilterType> = inner
                    .iter()
                    .filter_map(|p| Self::predicate_to_notion_filter(p, prop_name_map, schema))
                    .flat_map(Self::filter_to_filter_types)
                    .collect();
                if filter_types.is_empty() { None } else { Some(DbFilter::Or { or: filter_types }) }
            }
            _ => None,
        }
    }

    // ── Data fetch orchestration ──────────────────────────────────────

    async fn do_fetch(
        &self,
        table: &str,
        options: FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        match table {
            "pages" => self.fetch_pages(&options).await,
            "databases" => self.fetch_databases_table(&options).await,
            "users" => self.fetch_users(&options).await,
            _ if table.starts_with(DB_TABLE_PREFIX) => {
                self.fetch_database_rows(table, &options).await
            }
            _ => Err(ConnectorError::TableNotFound(table.to_string())),
        }
    }

    async fn fetch_pages(&self, options: &FetchOptions) -> ConnectorResult<Vec<RecordBatch>> {
        let full_schema = Self::get_static_schema("pages").unwrap();
        let schema = if let Some(ref proj) = options.projection {
            Self::apply_projection(full_schema, proj)
        } else {
            full_schema.clone()
        };
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let last_value_micros = options.last_value.as_deref()
            .and_then(Self::parse_notion_timestamp);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut cursor: Option<String> = None;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let response = call_with_retry(|| async {
                let req = SearchByTitleRequest {
                    filter: Some(SearchFilter {
                        value: FilterValue::Page,
                        property: FilterProperty::Object,
                    }),
                    sort: Some(SearchSort {
                        direction: SearchSortDirection::Descending,
                        timestamp: SearchTimestamp::LastEditedTime,
                    }),
                    start_cursor: cursor.clone(),
                    page_size: Some(100),
                    ..Default::default()
                };
                self.client.search.search_by_title(req).await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Notion search failed: {}", e)))?;

            let mut should_stop = false;
            for item in &response.results {
                if let PageOrDatabase::Page(page) = item {
                    if let Some(watermark) = last_value_micros {
                        if page.last_edited_time.timestamp_micros() <= watermark {
                            should_stop = true;
                            break;
                        }
                    }

                    Self::append_page_metadata(page, &schema, &mut builders);
                    total_rows += 1;

                    if total_rows >= max_rows {
                        break;
                    }
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
            }

            if should_stop || !response.has_more || response.next_cursor.is_none() {
                break;
            }

            cursor = response.next_cursor;
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }

    async fn fetch_databases_table(&self, options: &FetchOptions) -> ConnectorResult<Vec<RecordBatch>> {
        let full_schema = Self::get_static_schema("databases").unwrap();
        let schema = if let Some(ref proj) = options.projection {
            Self::apply_projection(full_schema, proj)
        } else {
            full_schema.clone()
        };
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let last_value_micros = options.last_value.as_deref()
            .and_then(Self::parse_notion_timestamp);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut cursor: Option<String> = None;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let response = call_with_retry(|| async {
                let req = SearchByTitleRequest {
                    filter: Some(SearchFilter {
                        value: FilterValue::Database,
                        property: FilterProperty::Object,
                    }),
                    sort: Some(SearchSort {
                        direction: SearchSortDirection::Descending,
                        timestamp: SearchTimestamp::LastEditedTime,
                    }),
                    start_cursor: cursor.clone(),
                    page_size: Some(100),
                    ..Default::default()
                };
                self.client.search.search_by_title(req).await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Notion search failed: {}", e)))?;

            let mut should_stop = false;
            for item in &response.results {
                if let PageOrDatabase::Database(db) = item {
                    if let Some(watermark) = last_value_micros {
                        if db.last_edited_time.timestamp_micros() <= watermark {
                            should_stop = true;
                            break;
                        }
                    }

                    Self::append_database_metadata(db, &schema, &mut builders);
                    total_rows += 1;

                    if total_rows >= max_rows {
                        break;
                    }
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
            }

            if should_stop || !response.has_more || response.next_cursor.is_none() {
                break;
            }

            cursor = response.next_cursor;
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }

    async fn fetch_users(&self, options: &FetchOptions) -> ConnectorResult<Vec<RecordBatch>> {
        let full_schema = Self::get_static_schema("users").unwrap();
        let schema = if let Some(ref proj) = options.projection {
            Self::apply_projection(full_schema, proj)
        } else {
            full_schema.clone()
        };
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut cursor: Option<String> = None;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let response = call_with_retry(|| async {
                self.client.users
                    .list_all_users(cursor.as_deref(), Some(100))
                    .await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Notion users list failed: {}", e)))?;

            for user in &response.results {
                Self::append_user(user, &schema, &mut builders);
                total_rows += 1;

                if total_rows >= max_rows {
                    break;
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
            }

            if !response.has_more || response.next_cursor.is_none() {
                break;
            }

            cursor = response.next_cursor;
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }

    async fn fetch_database_rows(
        &self,
        table: &str,
        options: &FetchOptions,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let raw_id = Self::extract_database_id(table)
            .ok_or_else(|| ConnectorError::TableNotFound(table.to_string()))?;

        let db_id = if raw_id.len() == 32 && !raw_id.contains('-') {
            format!(
                "{}-{}-{}-{}-{}",
                &raw_id[0..8], &raw_id[8..12], &raw_id[12..16],
                &raw_id[16..20], &raw_id[20..32]
            )
        } else {
            raw_id.to_string()
        };

        let db = call_with_retry(|| async {
            self.client.databases.retrieve_a_database(&db_id).await
        }).await
            .map_err(|e| ConnectorError::Internal(format!("Failed to retrieve database: {}", e)))?;

        let (full_schema, prop_name_map) = Self::build_schema_and_prop_map(&db);
        let schema = if let Some(ref proj) = options.projection {
            Self::apply_projection(&full_schema, proj)
        } else {
            full_schema.clone()
        };
        let arrow_schema = Arc::new(Self::to_arrow_schema(&schema));

        let mut filters: Vec<DbFilter> = Vec::new();

        if let Some(ref last_value) = options.last_value {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(last_value) {
                let utc_dt = dt.with_timezone(&chrono::Utc);
                filters.push(DbFilter::Value {
                    filter_type: FilterType::Timestamp {
                        timestamp: DbTimestamp::LastEditedTime,
                        condition: TimestampCondition::LastEditedTime(
                            DateCondition::After(utc_dt),
                        ),
                    },
                });
            }
        }

        if let Some(pred_filter) = Self::build_database_filter(&options.predicates, &prop_name_map, &full_schema) {
            filters.push(pred_filter);
        }

        let combined_filter = match filters.len() {
            0 => None,
            1 => Some(filters.into_iter().next().unwrap()),
            _ => {
                let filter_types: Vec<FilterType> = filters.into_iter()
                    .flat_map(Self::filter_to_filter_types)
                    .collect();
                Some(DbFilter::And { and: filter_types })
            }
        };

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
        let mut total_rows: usize = 0;
        let mut cursor: Option<String> = None;
        let max_rows = options.max_rows.unwrap_or(MAX_TOTAL_ROWS);

        loop {
            if total_rows >= max_rows {
                break;
            }

            let response = call_with_retry(|| async {
                let req = QueryDatabaseRequest {
                    filter: combined_filter.clone(),
                    sorts: Some(vec![DbSort::Timestamp {
                        timestamp: DbTimestamp::LastEditedTime,
                        direction: DbSortDirection::Descending,
                    }]),
                    start_cursor: cursor.clone(),
                    page_size: Some(100),
                };
                self.client.databases
                    .query_a_database(&db_id, req)
                    .await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Database query failed: {}", e)))?;

            for page in &response.results {
                Self::append_database_row(page, &schema, &prop_name_map, &mut builders);
                total_rows += 1;

                if total_rows >= max_rows {
                    break;
                }
            }

            if builders.len() >= BATCH_THRESHOLD {
                batches.push(builders.finish(arrow_schema.clone())?);
                builders = ColumnBuilders::new(&schema, BATCH_THRESHOLD);
            }

            if !response.has_more || response.next_cursor.is_none() {
                break;
            }

            cursor = response.next_cursor;
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        if builders.len() > 0 {
            batches.push(builders.finish(arrow_schema.clone())?);
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(arrow_schema)]);
        }

        Ok(batches)
    }

    /// Discover all databases the integration has access to.
    async fn discover_databases(&self) -> ConnectorResult<Vec<Database>> {
        let mut databases: Vec<Database> = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let response = call_with_retry(|| async {
                let req = SearchByTitleRequest {
                    filter: Some(SearchFilter {
                        value: FilterValue::Database,
                        property: FilterProperty::Object,
                    }),
                    start_cursor: cursor.clone(),
                    page_size: Some(100),
                    ..Default::default()
                };
                self.client.search.search_by_title(req).await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Notion search failed: {}", e)))?;

            for item in response.results {
                if let PageOrDatabase::Database(db) = item {
                    databases.push(db);
                }
            }

            if !response.has_more || response.next_cursor.is_none() {
                break;
            }

            cursor = response.next_cursor;
            tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        Ok(databases)
    }
}

#[async_trait]
impl Connector for NotionConnector {
    fn source_type(&self) -> SourceType {
        SourceType::Notion
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let mut tables: Vec<TableInfo> = Vec::new();

        for &name in STATIC_TABLES {
            if let Some(schema) = Self::get_static_schema(name) {
                let (supports_incremental, incremental_key) = match name {
                    "pages" | "databases" => (true, Some("last_edited_time".to_string())),
                    _ => (false, None),
                };

                tables.push(TableInfo {
                    name: name.to_string(),
                    schema: schema.clone(),
                    supports_incremental,
                    incremental_key,
                    estimated_rows: None,
                    primary_key_columns: vec!["id".to_string()],
                });
            }
        }

        let databases = self.discover_databases().await?;

        for db in &databases {
            let db_id = db.id.as_deref().unwrap_or("");
            if db_id.is_empty() {
                continue;
            }

            let (schema, _) = Self::build_schema_and_prop_map(db);
            let table_name = Self::database_table_id(db_id);

            tables.push(TableInfo {
                name: table_name,
                schema,
                supports_incremental: true,
                incremental_key: Some("last_edited_time".to_string()),
                estimated_rows: None,
                primary_key_columns: vec!["id".to_string()],
            });
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        if let Some(schema) = Self::get_static_schema(table) {
            return Ok(schema.clone());
        }

        if let Some(raw_id) = Self::extract_database_id(table) {
            let db_id = if raw_id.len() == 32 && !raw_id.contains('-') {
                format!(
                    "{}-{}-{}-{}-{}",
                    &raw_id[0..8], &raw_id[8..12], &raw_id[12..16],
                    &raw_id[16..20], &raw_id[20..32]
                )
            } else {
                raw_id.to_string()
            };

            let db = call_with_retry(|| async {
                self.client.databases.retrieve_a_database(&db_id).await
            }).await
                .map_err(|e| ConnectorError::Internal(format!("Failed to retrieve database: {}", e)))?;

            let (schema, _) = Self::build_schema_and_prop_map(&db);
            return Ok(schema);
        }

        Err(ConnectorError::TableNotFound(table.to_string()))
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batches = self.do_fetch(table, options).await?;
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        call_with_retry(|| async {
            self.client.users
                .list_all_users(None, Some(1))
                .await
        }).await
            .map_err(|e| ConnectorError::Authentication(format!("Notion auth failed: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_str::CompactString;

    #[test]
    fn test_notion_config_creation() {
        let config = NotionConfig::new("ntn_test_token_123");
        assert_eq!(config.api_token.expose(), "ntn_test_token_123");
    }

    #[test]
    fn test_notion_config_debug_redacts() {
        let config = NotionConfig::new("ntn_secret_token");
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("ntn_secret_token"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_static_schema_pages() {
        let schema = NotionConnector::get_static_schema("pages");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 10);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"title"));
        assert!(names.contains(&"created_time"));
        assert!(names.contains(&"last_edited_time"));
        assert!(names.contains(&"archived"));
        assert!(names.contains(&"url"));
        assert!(names.contains(&"parent_type"));
        assert!(names.contains(&"parent_id"));
    }

    #[test]
    fn test_static_schema_databases() {
        let schema = NotionConnector::get_static_schema("databases");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 10);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"title"));
        assert!(names.contains(&"description"));
        assert!(names.contains(&"is_inline"));
    }

    #[test]
    fn test_static_schema_users() {
        let schema = NotionConnector::get_static_schema("users");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.columns.len(), 5);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"user_type"));
        assert!(names.contains(&"avatar_url"));
    }

    #[test]
    fn test_static_schema_unknown() {
        assert!(NotionConnector::get_static_schema("nonexistent").is_none());
    }

    #[test]
    fn test_sanitize_property_name_basic() {
        assert_eq!(NotionConnector::sanitize_property_name("Name"), "name");
        assert_eq!(NotionConnector::sanitize_property_name("Status"), "status");
        assert_eq!(NotionConnector::sanitize_property_name("Due Date"), "due_date");
    }

    #[test]
    fn test_sanitize_property_name_special_chars() {
        assert_eq!(NotionConnector::sanitize_property_name("Price ($)"), "price");
        assert_eq!(NotionConnector::sanitize_property_name("Col (USD)"), "col__usd");
        assert_eq!(NotionConnector::sanitize_property_name("!@#"), "unnamed_property");
    }

    #[test]
    fn test_sanitize_property_name_leading_digit() {
        assert_eq!(NotionConnector::sanitize_property_name("123abc"), "col_123abc");
    }

    #[test]
    fn test_database_table_id() {
        assert_eq!(
            NotionConnector::database_table_id("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            "db_a1b2c3d4e5f67890abcdef1234567890"
        );
    }

    #[test]
    fn test_extract_database_id() {
        assert_eq!(
            NotionConnector::extract_database_id("db_a1b2c3d4e5f6"),
            Some("a1b2c3d4e5f6")
        );
        assert_eq!(NotionConnector::extract_database_id("pages"), None);
    }

    #[test]
    fn test_database_property_to_column_type() {
        use std::collections::HashMap;

        let title_prop = DatabaseProperty::Title {
            id: None,
            name: None,
            title: HashMap::new(),
        };
        assert_eq!(
            NotionConnector::database_property_to_column_type(&title_prop),
            Some(ColumnType::String)
        );

        let number_prop = DatabaseProperty::Number {
            id: None,
            name: None,
            number: notion_client::objects::database::NumberPropertyValue {
                format: notion_client::objects::database::NumberFormat::Number,
            },
        };
        assert_eq!(
            NotionConnector::database_property_to_column_type(&number_prop),
            Some(ColumnType::Float64)
        );

        let checkbox_prop = DatabaseProperty::Checkbox {
            id: None,
            name: None,
            checkbox: HashMap::new(),
        };
        assert_eq!(
            NotionConnector::database_property_to_column_type(&checkbox_prop),
            Some(ColumnType::Boolean)
        );

        let date_prop = DatabaseProperty::Date {
            id: None,
            name: None,
            date: HashMap::new(),
        };
        assert_eq!(
            NotionConnector::database_property_to_column_type(&date_prop),
            Some(ColumnType::Timestamp)
        );

        let button_prop = DatabaseProperty::Button {
            id: None,
            name: None,
            button: HashMap::new(),
        };
        assert_eq!(
            NotionConnector::database_property_to_column_type(&button_prop),
            None
        );
    }

    #[test]
    fn test_rich_text_to_plain() {
        let texts = vec![
            RichText::Text {
                text: notion_client::objects::rich_text::Text {
                    content: "Hello ".to_string(),
                    link: None,
                },
                annotations: None,
                plain_text: Some("Hello ".to_string()),
                href: None,
            },
            RichText::Text {
                text: notion_client::objects::rich_text::Text {
                    content: "World".to_string(),
                    link: None,
                },
                annotations: None,
                plain_text: Some("World".to_string()),
                href: None,
            },
        ];
        assert_eq!(NotionConnector::rich_text_to_plain(&texts), "Hello World");
    }

    #[test]
    fn test_rich_text_to_plain_empty() {
        let texts: Vec<RichText> = vec![];
        assert_eq!(NotionConnector::rich_text_to_plain(&texts), "");
    }

    #[test]
    fn test_rich_text_to_plain_with_none() {
        let texts = vec![RichText::None];
        assert_eq!(NotionConnector::rich_text_to_plain(&texts), "");
    }

    #[test]
    fn test_parent_type_str() {
        assert_eq!(
            NotionConnector::parent_type_str(&Parent::DatabaseId {
                database_id: "abc".to_string()
            }),
            "database"
        );
        assert_eq!(
            NotionConnector::parent_type_str(&Parent::PageId {
                page_id: "abc".to_string()
            }),
            "page"
        );
        assert_eq!(
            NotionConnector::parent_type_str(&Parent::Workspace { workspace: true }),
            "workspace"
        );
        assert_eq!(NotionConnector::parent_type_str(&Parent::None), "none");
    }

    #[test]
    fn test_parent_id_str() {
        assert_eq!(
            NotionConnector::parent_id_str(&Parent::DatabaseId {
                database_id: "db-123".to_string()
            }),
            "db-123"
        );
        assert_eq!(
            NotionConnector::parent_id_str(&Parent::PageId {
                page_id: "pg-456".to_string()
            }),
            "pg-456"
        );
        assert_eq!(
            NotionConnector::parent_id_str(&Parent::Workspace { workspace: true }),
            "workspace"
        );
        assert_eq!(NotionConnector::parent_id_str(&Parent::None), "");
    }

    #[test]
    fn test_parse_notion_timestamp_rfc3339() {
        let micros = NotionConnector::parse_notion_timestamp("2024-01-01T00:00:00Z");
        assert!(micros.is_some());
        assert_eq!(micros.unwrap(), 1704067200000000);
    }

    #[test]
    fn test_parse_notion_timestamp_with_offset() {
        let micros = NotionConnector::parse_notion_timestamp("2024-01-01T00:00:00+00:00");
        assert!(micros.is_some());
        assert_eq!(micros.unwrap(), 1704067200000000);
    }

    #[test]
    fn test_parse_notion_timestamp_date_only() {
        let micros = NotionConnector::parse_notion_timestamp("2024-01-01");
        assert!(micros.is_some());
        assert_eq!(micros.unwrap(), 1704067200000000);
    }

    #[test]
    fn test_parse_notion_timestamp_invalid() {
        assert!(NotionConnector::parse_notion_timestamp("not-a-date").is_none());
    }

    #[test]
    fn test_append_user_person() {
        let schema = NotionConnector::get_static_schema("users").unwrap();
        let arrow_schema = Arc::new(NotionConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);

        let user = notion_client::objects::user::User {
            object: "user".to_string(),
            id: "user-123".to_string(),
            user_type: Some(UserType::Person {
                person: notion_client::objects::user::Person {
                    email: "test@example.com".to_string(),
                },
            }),
            name: Some("Test User".to_string()),
            avatar_url: Some("https://example.com/avatar.png".to_string()),
        };

        NotionConnector::append_user(&user, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);

        let id_col = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(id_col.value(0), "user-123");
        let name_col = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(name_col.value(0), "Test User");
        let email_col = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(email_col.value(0), "test@example.com");
        let type_col = batch.column(3).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(type_col.value(0), "person");
        let avatar_col = batch.column(4).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(avatar_col.value(0), "https://example.com/avatar.png");
    }

    #[test]
    fn test_append_user_bot() {
        let schema = NotionConnector::get_static_schema("users").unwrap();
        let arrow_schema = Arc::new(NotionConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);

        let user = notion_client::objects::user::User {
            object: "user".to_string(),
            id: "bot-456".to_string(),
            user_type: Some(UserType::Bot {
                bot: notion_client::objects::user::Bot {
                    owner: notion_client::objects::user::OwnerType::Workspace { workspace: true },
                    workspace_name: "Test Workspace".to_string(),
                },
            }),
            name: Some("My Bot".to_string()),
            avatar_url: None,
        };

        NotionConnector::append_user(&user, &schema, &mut builders);
        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let type_col = batch.column(3).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(type_col.value(0), "bot");
        let email_col = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(arrow::array::Array::is_null(email_col, 0));
    }

    #[test]
    fn test_builders_users_batch() {
        let schema = NotionConnector::get_static_schema("users").unwrap();
        let arrow_schema = Arc::new(NotionConnector::to_arrow_schema(&schema));
        let mut builders = ColumnBuilders::new(&schema, 10);

        let users = vec![
            notion_client::objects::user::User {
                object: "user".to_string(),
                id: "user-1".to_string(),
                user_type: Some(UserType::Person {
                    person: notion_client::objects::user::Person {
                        email: "alice@example.com".to_string(),
                    },
                }),
                name: Some("Alice".to_string()),
                avatar_url: None,
            },
            notion_client::objects::user::User {
                object: "user".to_string(),
                id: "user-2".to_string(),
                user_type: Some(UserType::Bot {
                    bot: notion_client::objects::user::Bot {
                        owner: notion_client::objects::user::OwnerType::Workspace { workspace: true },
                        workspace_name: "ws".to_string(),
                    },
                }),
                name: Some("Bot".to_string()),
                avatar_url: None,
            },
        ];

        for user in &users {
            NotionConnector::append_user(user, &schema, &mut builders);
        }

        let batch = builders.finish(arrow_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_extract_page_property_title() {
        let prop = PageProperty::Title {
            id: None,
            title: vec![RichText::Text {
                text: notion_client::objects::rich_text::Text {
                    content: "Test Title".to_string(),
                    link: None,
                },
                annotations: None,
                plain_text: Some("Test Title".to_string()),
                href: None,
            }],
        };
        assert_eq!(
            NotionConnector::extract_page_property_string(&prop),
            Some("Test Title".to_string())
        );
    }

    #[test]
    fn test_extract_page_property_number() {
        let prop = PageProperty::Number {
            id: None,
            number: Some(serde_json::Number::from_f64(42.5).unwrap()),
        };
        assert_eq!(NotionConnector::extract_page_property_f64(&prop), Some(42.5));
    }

    #[test]
    fn test_extract_page_property_checkbox() {
        let prop = PageProperty::Checkbox {
            id: None,
            checkbox: true,
        };
        assert_eq!(NotionConnector::extract_page_property_bool(&prop), Some(true));
    }

    #[test]
    fn test_extract_page_property_select() {
        let prop = PageProperty::Select {
            id: None,
            select: Some(notion_client::objects::page::SelectPropertyValue {
                id: Some("sel-1".to_string()),
                name: Some("Option A".to_string()),
                color: None,
            }),
        };
        assert_eq!(
            NotionConnector::extract_page_property_string(&prop),
            Some("Option A".to_string())
        );
    }

    #[test]
    fn test_extract_page_property_multi_select() {
        let prop = PageProperty::MultiSelect {
            id: None,
            multi_select: vec![
                notion_client::objects::page::SelectPropertyValue {
                    id: Some("1".to_string()),
                    name: Some("Tag A".to_string()),
                    color: None,
                },
                notion_client::objects::page::SelectPropertyValue {
                    id: Some("2".to_string()),
                    name: Some("Tag B".to_string()),
                    color: None,
                },
            ],
        };
        assert_eq!(
            NotionConnector::extract_page_property_string(&prop),
            Some("Tag A, Tag B".to_string())
        );
    }

    #[test]
    fn test_predicate_equals_to_notion_filter() {
        let pred = Predicate::Equals {
            column: CompactString::from("status"),
            value: CompactString::from("Done"),
        };

        let mut prop_map = HashMap::new();
        prop_map.insert("status".to_string(), "Status".to_string());

        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("status", ColumnType::String, true),
            ],
        };

        let filter = NotionConnector::predicate_to_notion_filter(&pred, &prop_map, &schema);
        assert!(filter.is_some());
    }

    #[test]
    fn test_predicate_contains_to_notion_filter() {
        let pred = Predicate::Contains {
            column: CompactString::from("name"),
            substring: CompactString::from("test"),
        };

        let mut prop_map = HashMap::new();
        prop_map.insert("name".to_string(), "Name".to_string());

        let schema = TableSchema {
            columns: vec![
                ColumnSchema::new("name", ColumnType::String, true),
            ],
        };

        let filter = NotionConnector::predicate_to_notion_filter(&pred, &prop_map, &schema);
        assert!(filter.is_some());
    }

    #[test]
    fn test_predicate_unknown_column_returns_none() {
        let pred = Predicate::Equals {
            column: CompactString::from("nonexistent"),
            value: CompactString::from("value"),
        };

        let prop_map = HashMap::new();
        let schema = TableSchema { columns: vec![] };

        let filter = NotionConnector::predicate_to_notion_filter(&pred, &prop_map, &schema);
        assert!(filter.is_none());
    }

    #[test]
    fn test_predicate_unsupported_returns_none() {
        let pred = Predicate::IsNull {
            column: CompactString::from("email"),
            is_null: true,
        };

        let prop_map = HashMap::new();
        let schema = TableSchema { columns: vec![] };

        let filter = NotionConnector::predicate_to_notion_filter(&pred, &prop_map, &schema);
        assert!(filter.is_none());
    }

    #[test]
    fn test_build_schema_and_prop_map_base_columns() {
        let db = Database {
            id: Some("db-123".to_string()),
            created_time: chrono::Utc::now(),
            created_by: None,
            last_edited_time: chrono::Utc::now(),
            last_edited_by: None,
            title: vec![],
            description: vec![],
            icon: None,
            cover: None,
            properties: HashMap::new(),
            parent: Parent::Workspace { workspace: true },
            url: "https://notion.so/db-123".to_string(),
            archived: false,
            is_inline: false,
            public_url: None,
        };

        let (schema, prop_map) = NotionConnector::build_schema_and_prop_map(&db);
        assert_eq!(schema.columns.len(), 5);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"created_time"));
        assert!(names.contains(&"last_edited_time"));
        assert!(names.contains(&"url"));
        assert!(names.contains(&"archived"));
        assert!(prop_map.is_empty());
    }

    #[test]
    fn test_build_schema_and_prop_map_with_properties() {
        let mut properties = HashMap::new();
        properties.insert(
            "Name".to_string(),
            DatabaseProperty::Title {
                id: None,
                name: Some("Name".to_string()),
                title: HashMap::new(),
            },
        );
        properties.insert(
            "Price".to_string(),
            DatabaseProperty::Number {
                id: None,
                name: Some("Price".to_string()),
                number: notion_client::objects::database::NumberPropertyValue {
                    format: notion_client::objects::database::NumberFormat::Number,
                },
            },
        );
        properties.insert(
            "Done".to_string(),
            DatabaseProperty::Checkbox {
                id: None,
                name: Some("Done".to_string()),
                checkbox: HashMap::new(),
            },
        );

        let db = Database {
            id: Some("db-123".to_string()),
            created_time: chrono::Utc::now(),
            created_by: None,
            last_edited_time: chrono::Utc::now(),
            last_edited_by: None,
            title: vec![],
            description: vec![],
            icon: None,
            cover: None,
            properties,
            parent: Parent::Workspace { workspace: true },
            url: "https://notion.so/db-123".to_string(),
            archived: false,
            is_inline: false,
            public_url: None,
        };

        let (schema, prop_map) = NotionConnector::build_schema_and_prop_map(&db);
        assert_eq!(schema.columns.len(), 8);
        let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"price"));
        assert!(col_names.contains(&"done"));

        let price_col = schema.columns.iter().find(|c| c.name == "price").unwrap();
        assert_eq!(price_col.data_type, ColumnType::Float64);

        let done_col = schema.columns.iter().find(|c| c.name == "done").unwrap();
        assert_eq!(done_col.data_type, ColumnType::Boolean);

        assert_eq!(prop_map.get("name").unwrap(), "Name");
        assert_eq!(prop_map.get("price").unwrap(), "Price");
        assert_eq!(prop_map.get("done").unwrap(), "Done");
    }

    #[test]
    fn test_build_schema_and_prop_map_name_mapping() {
        let mut properties = HashMap::new();
        properties.insert(
            "Project Name".to_string(),
            DatabaseProperty::Title {
                id: None,
                name: Some("Project Name".to_string()),
                title: HashMap::new(),
            },
        );
        properties.insert(
            "Due Date".to_string(),
            DatabaseProperty::Date {
                id: None,
                name: Some("Due Date".to_string()),
                date: HashMap::new(),
            },
        );

        let db = Database {
            id: Some("db-123".to_string()),
            created_time: chrono::Utc::now(),
            created_by: None,
            last_edited_time: chrono::Utc::now(),
            last_edited_by: None,
            title: vec![],
            description: vec![],
            icon: None,
            cover: None,
            properties,
            parent: Parent::Workspace { workspace: true },
            url: "https://notion.so/db-123".to_string(),
            archived: false,
            is_inline: false,
            public_url: None,
        };

        let (schema, map) = NotionConnector::build_schema_and_prop_map(&db);
        assert!(map.contains_key("project_name"));
        assert_eq!(map.get("project_name").unwrap(), "Project Name");
        assert!(map.contains_key("due_date"));
        assert_eq!(map.get("due_date").unwrap(), "Due Date");

        let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"project_name"));
        assert!(col_names.contains(&"due_date"));
    }

    #[test]
    fn test_apply_projection_empty_returns_full_schema() {
        let schema = NotionConnector::get_static_schema("users").unwrap();
        let projected = NotionConnector::apply_projection(&schema, &[]);
        assert_eq!(projected.columns.len(), schema.columns.len());
    }

    #[test]
    fn test_apply_projection_filters_columns() {
        let schema = NotionConnector::get_static_schema("pages").unwrap();
        let projection = vec!["title".to_string(), "url".to_string()];
        let projected = NotionConnector::apply_projection(&schema, &projection);

        let names: Vec<&str> = projected.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"), "id must always be kept");
        assert!(names.contains(&"title"));
        assert!(names.contains(&"url"));
        assert!(!names.contains(&"archived"));
        assert!(!names.contains(&"parent_type"));
        assert_eq!(projected.columns.len(), 3);
    }

    #[test]
    fn test_apply_projection_nonexistent_columns() {
        let schema = NotionConnector::get_static_schema("users").unwrap();
        let projection = vec!["nonexistent".to_string()];
        let projected = NotionConnector::apply_projection(&schema, &projection);

        let names: Vec<&str> = projected.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id"], "only id should survive");
    }

    #[test]
    fn test_is_rate_limited_429() {
        let err = NotionClientError::InvalidStatusCode {
            error: notion_client::objects::error::Error {
                object: "error".to_string(),
                status: 429,
                code: "rate_limited".to_string(),
                message: "Rate limited".to_string(),
                request_id: None,
            },
        };
        assert!(is_rate_limited(&err));
    }

    #[test]
    fn test_is_rate_limited_other_status() {
        let err = NotionClientError::InvalidStatusCode {
            error: notion_client::objects::error::Error {
                object: "error".to_string(),
                status: 404,
                code: "object_not_found".to_string(),
                message: "Not found".to_string(),
                request_id: None,
            },
        };
        assert!(!is_rate_limited(&err));
    }
}
