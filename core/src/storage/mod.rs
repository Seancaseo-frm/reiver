//! Asset Storage Module
//!
//! Provides a trait-based abstraction for storing binary assets (images, audio, PDFs)
//! used in multimodal prompt configurations.
//!
//! Implementations:
//! - `InMemoryStorage`: For unit tests (HashMap-based, no persistence)
//! - `LocalFileStorage`: For development (stores in local filesystem)
//! - `S3Storage`: For production (S3-compatible object storage)

mod in_memory;
mod local_file;
mod s3;

use async_trait::async_trait;
use std::time::Duration;

pub use in_memory::InMemoryStorage;
pub use local_file::LocalFileStorage;
pub use s3::S3Storage;

/// Maximum total asset size per prompt version (20MB)
pub const MAX_TOTAL_ASSET_SIZE: usize = 20 * 1024 * 1024;

/// Supported asset content types
pub const SUPPORTED_IMAGE_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
];

pub const SUPPORTED_AUDIO_TYPES: &[&str] = &["audio/mp3", "audio/wav", "audio/mpeg"];

pub const SUPPORTED_DOCUMENT_TYPES: &[&str] = &["application/pdf", "text/plain"];

/// Error type for storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Asset not found: {0}")]
    NotFound(String),

    #[error("Storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Asset too large: {size} bytes exceeds limit of {limit} bytes")]
    TooLarge { size: usize, limit: usize },

    #[error("Unsupported content type: {0}")]
    UnsupportedContentType(String),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

/// Result type for storage operations
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Metadata about a stored asset
#[derive(Debug, Clone)]
pub struct AssetMetadata {
    /// Storage key for the asset
    pub key: String,
    /// MIME content type (e.g., "image/png")
    pub content_type: String,
    /// Size in bytes
    pub size: usize,
}

/// Trait for asset storage backends
///
/// All implementations must be thread-safe (Send + Sync) for use across async tasks.
#[async_trait]
pub trait AssetStorage: Send + Sync {
    /// Store an asset and return a storage key.
    ///
    /// # Arguments
    /// * `key` - Unique identifier for the asset (typically `{project_id}/{version_id}/{filename}`)
    /// * `data` - Raw binary data
    /// * `content_type` - MIME type of the content
    ///
    /// # Returns
    /// The storage key that can be used to retrieve the asset
    async fn put(&self, key: &str, data: &[u8], content_type: &str) -> StorageResult<String>;

    /// Retrieve an asset by key.
    ///
    /// # Returns
    /// The raw binary data
    async fn get(&self, key: &str) -> StorageResult<Vec<u8>>;

    /// Delete an asset.
    async fn delete(&self, key: &str) -> StorageResult<()>;

    /// Get a URL for the asset (for injection into LLM requests).
    ///
    /// For local storage, this returns an internal URL.
    /// For S3, this generates a presigned URL.
    ///
    /// # Arguments
    /// * `key` - The storage key
    /// * `expires_in` - How long the URL should be valid (for presigned URLs)
    async fn get_url(&self, key: &str, expires_in: Duration) -> StorageResult<String>;

    /// Check if an asset exists.
    async fn exists(&self, key: &str) -> StorageResult<bool>;
}

/// Validate that the content type is supported for prompt assets.
pub fn validate_content_type(content_type: &str) -> StorageResult<()> {
    let is_supported = SUPPORTED_IMAGE_TYPES.contains(&content_type)
        || SUPPORTED_AUDIO_TYPES.contains(&content_type)
        || SUPPORTED_DOCUMENT_TYPES.contains(&content_type);

    if !is_supported {
        return Err(StorageError::UnsupportedContentType(
            content_type.to_string(),
        ));
    }
    Ok(())
}

/// Get the category of a content type (image, audio, or document).
pub fn get_content_category(content_type: &str) -> Option<&'static str> {
    if SUPPORTED_IMAGE_TYPES.contains(&content_type) {
        Some("image")
    } else if SUPPORTED_AUDIO_TYPES.contains(&content_type) {
        Some("audio")
    } else if SUPPORTED_DOCUMENT_TYPES.contains(&content_type) {
        Some("document")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_content_type() {
        assert!(validate_content_type("image/png").is_ok());
        assert!(validate_content_type("image/jpeg").is_ok());
        assert!(validate_content_type("audio/mp3").is_ok());
        assert!(validate_content_type("application/pdf").is_ok());
        assert!(validate_content_type("application/json").is_err());
        assert!(validate_content_type("text/html").is_err());
    }

    #[test]
    fn test_get_content_category() {
        assert_eq!(get_content_category("image/png"), Some("image"));
        assert_eq!(get_content_category("audio/wav"), Some("audio"));
        assert_eq!(get_content_category("application/pdf"), Some("document"));
        assert_eq!(get_content_category("text/html"), None);
    }
}
