//! S3-Compatible Asset Storage
//!
//! Production storage backend using S3-compatible object storage.
//! Compatible with AWS S3, MinIO, Cloudflare R2, etc.

use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::time::Duration;

use super::{
    validate_content_type, AssetStorage, StorageError, StorageResult, MAX_TOTAL_ASSET_SIZE,
};

/// S3 storage configuration
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Optional custom endpoint for S3-compatible services (MinIO, R2)
    pub endpoint: Option<String>,
    /// Whether to use path-style addressing (required for some S3-compatible services)
    pub path_style: bool,
}

impl S3Config {
    /// Create a new S3 configuration for AWS S3.
    pub fn aws(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            path_style: false,
        }
    }

    /// Create a configuration for MinIO or other S3-compatible services.
    pub fn s3_compatible(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: Some(endpoint.into()),
            path_style: true,
        }
    }
}

/// S3 storage backend for production use.
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    /// Create a new S3 storage instance from configuration.
    pub async fn new(config: S3Config) -> StorageResult<Self> {
        let mut aws_config_builder = aws_config::from_env().region(Region::new(config.region));

        // Apply custom endpoint if provided
        if let Some(endpoint) = &config.endpoint {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint.clone());
        }

        let aws_config = aws_config_builder.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(config.path_style)
            .build();

        let client = Client::from_conf(s3_config);

        Ok(Self {
            client,
            bucket: config.bucket,
        })
    }

    /// Create from environment variables for simpler configuration.
    ///
    /// Expects:
    /// - `ASSET_S3_BUCKET`: Bucket name (required)
    /// - `ASSET_S3_REGION`: AWS region (defaults to "us-east-1")
    /// - `ASSET_S3_ENDPOINT`: Custom endpoint (optional)
    /// - `ASSET_S3_PATH_STYLE`: "true" for path-style (optional)
    pub async fn from_env() -> StorageResult<Self> {
        let bucket = std::env::var("ASSET_S3_BUCKET").map_err(|_| {
            StorageError::Config("ASSET_S3_BUCKET environment variable not set".to_string())
        })?;

        let region = std::env::var("ASSET_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let endpoint = std::env::var("ASSET_S3_ENDPOINT").ok();

        let path_style = std::env::var("ASSET_S3_PATH_STYLE")
            .map(|v| v == "true")
            .unwrap_or(false);

        let config = S3Config {
            bucket,
            region,
            endpoint,
            path_style,
        };

        Self::new(config).await
    }
}

#[async_trait]
impl AssetStorage for S3Storage {
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

        let body = ByteStream::from(data.to_vec());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        Ok(key.to_string())
    }

    async fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                // Check if it's a NotFound error
                let service_error = e.into_service_error();
                if service_error.is_no_such_key() {
                    StorageError::NotFound(key.to_string())
                } else {
                    StorageError::S3(service_error.to_string())
                }
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        Ok(())
    }

    async fn get_url(&self, key: &str, expires_in: Duration) -> StorageResult<String> {
        let presigning_config = PresigningConfig::builder()
            .expires_in(expires_in)
            .build()
            .map_err(|e| StorageError::S3(e.to_string()))?;

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(presigning_config)
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;

        Ok(presigned.uri().to_string())
    }

    async fn exists(&self, key: &str) -> StorageResult<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    Ok(false)
                } else {
                    Err(StorageError::S3(service_error.to_string()))
                }
            }
        }
    }
}

// Note: S3 tests require a real S3 or MinIO instance, so they are typically
// integration tests rather than unit tests. The in-memory and local file
// implementations can be used for unit testing the storage trait behavior.
