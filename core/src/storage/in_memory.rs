//! In-Memory Asset Storage
//!
//! A HashMap-based implementation of AssetStorage for unit testing.
//! No persistence - all data is lost when the process exits.

use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Duration;

use super::{
    validate_content_type, AssetStorage, StorageError, StorageResult, MAX_TOTAL_ASSET_SIZE,
};

/// Stored asset data with metadata
struct StoredAsset {
    data: Vec<u8>,
    content_type: String,
}

/// In-memory storage backend for testing.
///
/// Thread-safe via RwLock, suitable for use in async contexts.
pub struct InMemoryStorage {
    assets: RwLock<HashMap<String, StoredAsset>>,
    /// Base URL for generating asset URLs (e.g., "http://localhost:3000/assets")
    base_url: String,
}

impl InMemoryStorage {
    /// Create a new in-memory storage instance.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            assets: RwLock::new(HashMap::new()),
            base_url: base_url.into(),
        }
    }

    /// Create with default test URL.
    pub fn new_for_tests() -> Self {
        Self::new("http://localhost:3000/assets")
    }

    /// Get all stored keys (useful for testing).
    pub fn keys(&self) -> Vec<String> {
        self.assets.read().keys().cloned().collect()
    }

    /// Clear all stored assets (useful for testing).
    pub fn clear(&self) {
        self.assets.write().clear();
    }

    /// Get the count of stored assets.
    pub fn len(&self) -> usize {
        self.assets.read().len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.assets.read().is_empty()
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new_for_tests()
    }
}

#[async_trait]
impl AssetStorage for InMemoryStorage {
    async fn put(&self, key: &str, data: &[u8], content_type: &str) -> StorageResult<String> {
        // Validate content type
        validate_content_type(content_type)?;

        // Validate size
        if data.len() > MAX_TOTAL_ASSET_SIZE {
            return Err(StorageError::TooLarge {
                size: data.len(),
                limit: MAX_TOTAL_ASSET_SIZE,
            });
        }

        let asset = StoredAsset {
            data: data.to_vec(),
            content_type: content_type.to_string(),
        };

        self.assets.write().insert(key.to_string(), asset);
        Ok(key.to_string())
    }

    async fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        self.assets
            .read()
            .get(key)
            .map(|a| a.data.clone())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.assets.write().remove(key);
        Ok(())
    }

    async fn get_url(&self, key: &str, _expires_in: Duration) -> StorageResult<String> {
        // Verify asset exists
        if !self.assets.read().contains_key(key) {
            return Err(StorageError::NotFound(key.to_string()));
        }

        // Return a mock URL - in-memory storage doesn't have real URLs
        Ok(format!("{}/{}", self.base_url, key))
    }

    async fn exists(&self, key: &str) -> StorageResult<bool> {
        Ok(self.assets.read().contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let storage = InMemoryStorage::new_for_tests();
        let key = "test/asset.png";
        let data = b"fake image data";

        storage.put(key, data, "image/png").await.unwrap();
        let retrieved = storage.get(key).await.unwrap();

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = InMemoryStorage::new_for_tests();
        let key = "test/asset.png";

        storage.put(key, b"data", "image/png").await.unwrap();
        assert!(storage.exists(key).await.unwrap());

        storage.delete(key).await.unwrap();
        assert!(!storage.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_url() {
        let storage = InMemoryStorage::new("https://example.com/assets");
        let key = "project/version/image.png";

        storage.put(key, b"data", "image/png").await.unwrap();
        let url = storage
            .get_url(key, Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(url, "https://example.com/assets/project/version/image.png");
    }

    #[tokio::test]
    async fn test_not_found() {
        let storage = InMemoryStorage::new_for_tests();
        let result = storage.get("nonexistent").await;

        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_unsupported_content_type() {
        let storage = InMemoryStorage::new_for_tests();
        let result = storage.put("test.json", b"data", "application/json").await;
        assert!(matches!(
            result,
            Err(StorageError::UnsupportedContentType(_))
        ));
    }
}
