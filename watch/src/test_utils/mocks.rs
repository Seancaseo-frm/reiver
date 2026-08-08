//! Mock implementations for external services
//!
//! Provides mock traits and implementations for testing without real services.

use anyhow::Result;
use base64::Engine;
use mockall::automock;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

// ============================================================================
// Database Mocks
// ============================================================================

/// Trait for PostgreSQL database operations
#[automock]
#[async_trait::async_trait]
pub trait DatabaseOperations: Send + Sync {
    async fn query_user_by_email(&self, email: &str) -> Result<Option<UserRow>>;
    async fn query_user_by_id(&self, id: Uuid) -> Result<Option<UserRow>>;
    async fn create_user(&self, email: &str, password_hash: &str) -> Result<UserRow>;
    async fn query_project(&self, id: Uuid) -> Result<Option<ProjectRow>>;
    async fn query_project_by_key(&self, key: &str) -> Result<Option<ProjectRow>>;
    async fn list_projects_for_org(&self, org_id: Uuid) -> Result<Vec<ProjectRow>>;
}

#[derive(Debug, Clone)]
pub struct UserRow {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct ProjectRow {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub name: String,
    pub project_key: String,
}

// ============================================================================
// Redis Mocks
// ============================================================================

/// Trait for Redis operations
#[automock]
#[async_trait::async_trait]
pub trait RedisOperations: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn set(&self, key: &str, value: &str) -> Result<()>;
    async fn setex(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()>;
    async fn incr(&self, key: &str) -> Result<i64>;
    async fn expire(&self, key: &str, ttl_seconds: i64) -> Result<()>;
    async fn del(&self, key: &str) -> Result<()>;
    async fn ttl(&self, key: &str) -> Result<i64>;
}

/// In-memory Redis mock for testing
pub struct InMemoryRedis {
    data: Arc<RwLock<HashMap<String, (String, Option<std::time::Instant>)>>>,
}

impl InMemoryRedis {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();
        data.get(key).and_then(|(value, expiry)| {
            if let Some(exp) = expiry {
                if std::time::Instant::now() > *exp {
                    return None;
                }
            }
            Some(value.clone())
        })
    }

    pub fn set(&self, key: &str, value: &str) {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_string(), (value.to_string(), None));
    }

    pub fn setex(&self, key: &str, value: &str, ttl_seconds: u64) {
        let mut data = self.data.write().unwrap();
        let expiry = std::time::Instant::now() + std::time::Duration::from_secs(ttl_seconds);
        data.insert(key.to_string(), (value.to_string(), Some(expiry)));
    }

    pub fn incr(&self, key: &str) -> i64 {
        let mut data = self.data.write().unwrap();
        let current = data
            .get(key)
            .and_then(|(v, _)| v.parse::<i64>().ok())
            .unwrap_or(0);
        let new_value = current + 1;
        data.insert(key.to_string(), (new_value.to_string(), None));
        new_value
    }

    pub fn del(&self, key: &str) {
        let mut data = self.data.write().unwrap();
        data.remove(key);
    }

    pub fn clear(&self) {
        let mut data = self.data.write().unwrap();
        data.clear();
    }
}

impl Default for InMemoryRedis {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Kafka Mocks
// ============================================================================

/// Trait for Kafka operations
#[automock]
#[async_trait::async_trait]
pub trait KafkaOperations: Send + Sync {
    async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<()>;
    async fn send_batch(&self, topic: &str, messages: Vec<(String, Vec<u8>)>) -> Result<()>;
}

/// In-memory Kafka mock for testing
pub struct InMemoryKafka {
    messages: Arc<RwLock<HashMap<String, Vec<(String, Vec<u8>)>>>>,
}

impl InMemoryKafka {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn send(&self, topic: &str, key: &str, payload: &[u8]) {
        let mut messages = self.messages.write().unwrap();
        messages
            .entry(topic.to_string())
            .or_insert_with(Vec::new)
            .push((key.to_string(), payload.to_vec()));
    }

    pub fn get_messages(&self, topic: &str) -> Vec<(String, Vec<u8>)> {
        let messages = self.messages.read().unwrap();
        messages.get(topic).cloned().unwrap_or_default()
    }

    pub fn message_count(&self, topic: &str) -> usize {
        let messages = self.messages.read().unwrap();
        messages.get(topic).map(|v| v.len()).unwrap_or(0)
    }

    pub fn clear(&self) {
        let mut messages = self.messages.write().unwrap();
        messages.clear();
    }
}

impl Default for InMemoryKafka {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ClickHouse Mocks
// ============================================================================

/// Trait for ClickHouse operations
#[automock]
#[async_trait::async_trait]
pub trait ClickHouseOperations: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn query_json(&self, sql: &str) -> Result<Vec<serde_json::Value>>;
    async fn insert_json(&self, table: &str, rows: Vec<serde_json::Value>) -> Result<()>;
}

/// In-memory ClickHouse mock for testing
pub struct InMemoryClickHouse {
    tables: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    queries: Arc<RwLock<Vec<String>>>,
}

impl InMemoryClickHouse {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            queries: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn insert(&self, table: &str, row: serde_json::Value) {
        let mut tables = self.tables.write().unwrap();
        tables
            .entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(row);
    }

    pub fn get_rows(&self, table: &str) -> Vec<serde_json::Value> {
        let tables = self.tables.read().unwrap();
        tables.get(table).cloned().unwrap_or_default()
    }

    pub fn row_count(&self, table: &str) -> usize {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|v| v.len()).unwrap_or(0)
    }

    pub fn record_query(&self, sql: &str) {
        let mut queries = self.queries.write().unwrap();
        queries.push(sql.to_string());
    }

    pub fn get_queries(&self) -> Vec<String> {
        let queries = self.queries.read().unwrap();
        queries.clone()
    }

    pub fn clear(&self) {
        let mut tables = self.tables.write().unwrap();
        let mut queries = self.queries.write().unwrap();
        tables.clear();
        queries.clear();
    }
}

impl Default for InMemoryClickHouse {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// HTTP Client Mocks
// ============================================================================

/// Trait for HTTP client operations
#[automock]
#[async_trait::async_trait]
pub trait HttpClient: Send + Sync {
    async fn get(&self, url: &str) -> Result<HttpResponse>;
    async fn post(&self, url: &str, body: &str) -> Result<HttpResponse>;
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
    pub headers: HashMap<String, String>,
}

impl HttpResponse {
    pub fn ok(body: &str) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            headers: HashMap::new(),
        }
    }

    pub fn json(status: u16, body: serde_json::Value) -> Self {
        Self {
            status,
            body: body.to_string(),
            headers: HashMap::new(),
        }
    }

    pub fn not_found() -> Self {
        Self {
            status: 404,
            body: "Not Found".to_string(),
            headers: HashMap::new(),
        }
    }

    pub fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            body: message.to_string(),
            headers: HashMap::new(),
        }
    }
}

// ============================================================================
// Encryption Mocks
// ============================================================================

/// A no-op encryptor for testing that doesn't actually encrypt
pub struct TestEncryptor;

impl TestEncryptor {
    pub fn new() -> Self {
        Self
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        // Just base64 encode for testing (NOT SECURE - test only!)
        Ok(base64::engine::general_purpose::STANDARD.encode(plaintext))
    }

    pub fn decrypt(&self, ciphertext: &str) -> Result<String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext)
            .map_err(|e| anyhow::anyhow!("Decode error: {}", e))?;
        String::from_utf8(bytes).map_err(|e| anyhow::anyhow!("UTF-8 error: {}", e))
    }
}

impl Default for TestEncryptor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_redis() {
        let redis = InMemoryRedis::new();

        redis.set("key1", "value1");
        assert_eq!(redis.get("key1"), Some("value1".to_string()));

        redis.del("key1");
        assert_eq!(redis.get("key1"), None);
    }

    #[test]
    fn test_in_memory_redis_incr() {
        let redis = InMemoryRedis::new();

        assert_eq!(redis.incr("counter"), 1);
        assert_eq!(redis.incr("counter"), 2);
        assert_eq!(redis.incr("counter"), 3);
    }

    #[test]
    fn test_in_memory_kafka() {
        let kafka = InMemoryKafka::new();

        kafka.send("topic1", "key1", b"message1");
        kafka.send("topic1", "key2", b"message2");

        assert_eq!(kafka.message_count("topic1"), 2);
        assert_eq!(kafka.message_count("topic2"), 0);
    }

    #[test]
    fn test_in_memory_clickhouse() {
        let ch = InMemoryClickHouse::new();

        ch.insert("errors", serde_json::json!({"id": 1}));
        ch.insert("errors", serde_json::json!({"id": 2}));

        assert_eq!(ch.row_count("errors"), 2);
        assert_eq!(ch.row_count("spans"), 0);
    }

    #[test]
    fn test_test_encryptor() {
        let enc = TestEncryptor::new();

        let plaintext = "secret_value";
        let encrypted = enc.encrypt(plaintext).unwrap();
        let decrypted = enc.decrypt(&encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }
}
