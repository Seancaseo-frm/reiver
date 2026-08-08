//! Local Filesystem Asset Storage
//!
//! Stores assets in the local filesystem for development use.
//! Easy to migrate to S3 later by swapping the implementation.

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{
    validate_content_type, AssetStorage, StorageError, StorageResult, MAX_TOTAL_ASSET_SIZE,
};

/// Normalize a path by resolving `.` and `..` components lexically.
/// Unlike `canonicalize()`, this doesn't require the path to exist.
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                // Pop the last component if possible
                components.pop();
            }
            std::path::Component::CurDir => {
                // Skip current directory references
            }
            c => {
                components.push(c);
            }
        }
    }
    components.iter().collect()
}

/// Local filesystem storage backend for development.
///
/// Stores files in a directory structure:
/// `{base_path}/{key}`
///
/// Where key is typically `{project_id}/{version_id}/{filename}`
pub struct LocalFileStorage {
    /// Base directory for storing assets (e.g., "./data/assets")
    base_path: PathBuf,
    /// Base URL for generating asset URLs (e.g., "http://localhost:3000/api/assets")
    base_url: String,
}

impl LocalFileStorage {
    /// Create a new local file storage instance.
    ///
    /// # Arguments
    /// * `base_path` - Directory where assets will be stored
    /// * `base_url` - Base URL for generating asset URLs
    pub fn new(base_path: impl AsRef<Path>, base_url: impl Into<String>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            base_url: base_url.into(),
        }
    }

    /// Get the full filesystem path for a key.
    ///
    /// # Security
    /// This method validates that the resolved path stays within the base directory
    /// to prevent path traversal attacks (e.g., keys like "../../../etc/passwd").
    fn get_path(&self, key: &str) -> StorageResult<PathBuf> {
        // Reject keys with obvious path traversal patterns early
        if key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
            return Err(StorageError::Config(format!(
                "Invalid storage key: contains path traversal characters"
            )));
        }

        let path = self.base_path.join(key);

        // Normalize the path and verify it's still within base_path
        // We use lexical normalization since the file may not exist yet
        let normalized = normalize_path(&path);
        let normalized_base = normalize_path(&self.base_path);

        if !normalized.starts_with(&normalized_base) {
            return Err(StorageError::Config(format!(
                "Invalid storage key: path traversal detected"
            )));
        }

        Ok(path)
    }

    /// Ensure the parent directory exists for a given path.
    async fn ensure_parent_dir(&self, path: &Path) -> StorageResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AssetStorage for LocalFileStorage {
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

        let path = self.get_path(key)?;
        self.ensure_parent_dir(&path).await?;

        // Write atomically using a temp file
        let temp_path = path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(data).await?;
        file.sync_all().await?;

        // Rename to final path
        fs::rename(&temp_path, &path).await?;

        Ok(key.to_string())
    }

    async fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let path = self.get_path(key)?;
        fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let path = self.get_path(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Idempotent delete
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn get_url(&self, key: &str, _expires_in: Duration) -> StorageResult<String> {
        // Verify asset exists
        if !self.exists(key).await? {
            return Err(StorageError::NotFound(key.to_string()));
        }

        // For local storage, URLs don't expire
        Ok(format!("{}/{}", self.base_url, key))
    }

    async fn exists(&self, key: &str) -> StorageResult<bool> {
        let path = self.get_path(key)?;
        Ok(path.exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup() -> (LocalFileStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalFileStorage::new(temp_dir.path(), "http://localhost:3000/assets");
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (storage, _temp_dir) = setup().await;
        let key = "project1/version1/image.png";
        let data = b"fake image data";

        storage.put(key, data, "image/png").await.unwrap();
        let retrieved = storage.get(key).await.unwrap();

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_creates_parent_directories() {
        let (storage, _temp_dir) = setup().await;
        let key = "deep/nested/path/to/asset.png";

        storage.put(key, b"data", "image/png").await.unwrap();
        assert!(storage.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let (storage, _temp_dir) = setup().await;
        let key = "test/asset.png";

        storage.put(key, b"data", "image/png").await.unwrap();
        assert!(storage.exists(key).await.unwrap());

        storage.delete(key).await.unwrap();
        assert!(!storage.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_is_ok() {
        let (storage, _temp_dir) = setup().await;
        // Should not error on deleting non-existent file
        storage.delete("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_get_url() {
        let (storage, _temp_dir) = setup().await;
        let key = "project/version/image.png";

        storage.put(key, b"data", "image/png").await.unwrap();
        let url = storage
            .get_url(key, Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(
            url,
            "http://localhost:3000/assets/project/version/image.png"
        );
    }

    #[tokio::test]
    async fn test_not_found() {
        let (storage, _temp_dir) = setup().await;
        let result = storage.get("nonexistent").await;

        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_size_limit() {
        let (storage, _temp_dir) = setup().await;
        let large_data = vec![0u8; MAX_TOTAL_ASSET_SIZE + 1];

        let result = storage.put("test.bin", &large_data, "image/png").await;
        assert!(matches!(result, Err(StorageError::TooLarge { .. })));
    }

    #[tokio::test]
    async fn test_unsupported_content_type() {
        let (storage, _temp_dir) = setup().await;
        let result = storage.put("test.json", b"data", "application/json").await;
        assert!(matches!(
            result,
            Err(StorageError::UnsupportedContentType(_))
        ));
    }

    #[tokio::test]
    async fn test_path_traversal_protection() {
        let (storage, _temp_dir) = setup().await;

        // Test various path traversal attempts
        let malicious_keys = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32\\config\\sam",
            "foo/../../../etc/passwd",
            "/etc/passwd",
            "\\windows\\system32",
            "project/../../etc/passwd",
        ];

        for key in malicious_keys {
            let result = storage.put(key, b"malicious data", "text/plain").await;
            assert!(
                matches!(result, Err(StorageError::Config(_))),
                "Path traversal should be blocked for key: {}",
                key
            );

            let result = storage.get(key).await;
            assert!(
                matches!(result, Err(StorageError::Config(_))),
                "Path traversal should be blocked for key: {}",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_valid_nested_paths() {
        let (storage, _temp_dir) = setup().await;

        // These should all work fine
        let valid_keys = [
            "project/version/file.png",
            "a/b/c/d/e/file.txt",
            "uuid-here/another-uuid/image.jpg",
        ];

        for key in valid_keys {
            storage.put(key, b"data", "image/png").await.unwrap();
            assert!(storage.exists(key).await.unwrap());
        }
    }
}
