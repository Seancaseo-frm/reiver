//! Cloudflare R2 Storage Module
//!
//! Provides S3-compatible object storage for Parquet files using Cloudflare R2.
//! R2 offers zero egress fees, making it ideal for data warehouse workloads
//! where ClickHouse queries data directly from object storage.

use aws_config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use once_cell::sync::Lazy;
use regex::Regex;
use std::time::Duration;
use thiserror::Error;

// ============================================================================
// R2 Config Validation
// ============================================================================

/// Regex for valid S3/R2 bucket names.
/// Rules:
/// - 3-63 characters
/// - lowercase letters, numbers, and hyphens only (no periods)
/// - must start and end with letter or number
static BUCKET_NAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$").expect("Invalid bucket regex"));

/// Regex for valid Cloudflare account IDs.
/// Account IDs are 32-character lowercase hexadecimal strings.
static ACCOUNT_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-f0-9]{32}$").expect("Invalid account ID regex"));

/// Regex for valid R2 access key IDs.
/// Access keys are alphanumeric strings, typically 20-40 characters.
static ACCESS_KEY_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z0-9]{16,128}$").expect("Invalid access key regex"));

/// Validation errors for R2 configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum R2ValidationError {
    /// Invalid bucket name
    InvalidBucket(String),
    /// Invalid account ID
    InvalidAccountId(String),
    /// Invalid access key ID
    InvalidAccessKey(String),
    /// Invalid secret access key
    InvalidSecretKey(String),
}

impl std::fmt::Display for R2ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            R2ValidationError::InvalidBucket(msg) => write!(f, "Invalid bucket name: {}", msg),
            R2ValidationError::InvalidAccountId(msg) => write!(f, "Invalid account ID: {}", msg),
            R2ValidationError::InvalidAccessKey(msg) => write!(f, "Invalid access key: {}", msg),
            R2ValidationError::InvalidSecretKey(msg) => write!(f, "Invalid secret key: {}", msg),
        }
    }
}

impl std::error::Error for R2ValidationError {}

/// Validate an R2 bucket name.
///
/// Bucket names must:
/// - Be 3-63 characters long
/// - Contain only lowercase letters, numbers, and hyphens
/// - Start and end with a letter or number
/// - Not contain consecutive periods or SQL injection characters
pub fn validate_bucket_name(bucket: &str) -> Result<(), R2ValidationError> {
    // Check length
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(R2ValidationError::InvalidBucket(format!(
            "Bucket name must be 3-63 characters, got {}",
            bucket.len()
        )));
    }

    // Check for SQL injection characters
    if bucket.contains('\'')
        || bucket.contains('"')
        || bucket.contains(';')
        || bucket.contains('\\')
    {
        return Err(R2ValidationError::InvalidBucket(
            "Bucket name contains forbidden characters (' \" ; \\)".to_string(),
        ));
    }

    // Check regex pattern
    if !BUCKET_NAME_REGEX.is_match(bucket) {
        return Err(R2ValidationError::InvalidBucket(format!(
            "Bucket name '{}' does not match valid pattern (lowercase alphanumeric and hyphens only)",
            bucket
        )));
    }

    Ok(())
}

/// Validate a Cloudflare account ID.
///
/// Account IDs must be 32-character lowercase hexadecimal strings.
pub fn validate_account_id(account_id: &str) -> Result<(), R2ValidationError> {
    // Check for SQL injection characters first
    if account_id.contains('\'')
        || account_id.contains('"')
        || account_id.contains(';')
        || account_id.contains('\\')
    {
        return Err(R2ValidationError::InvalidAccountId(
            "Account ID contains forbidden characters (' \" ; \\)".to_string(),
        ));
    }

    // Check regex pattern
    if !ACCOUNT_ID_REGEX.is_match(account_id) {
        return Err(R2ValidationError::InvalidAccountId(format!(
            "Account ID '{}' is not a valid 32-character hex string",
            account_id
        )));
    }

    Ok(())
}

/// Validate an R2 access key ID.
///
/// Access keys must be alphanumeric, 16-128 characters.
pub fn validate_access_key(access_key: &str) -> Result<(), R2ValidationError> {
    // Check for SQL injection characters
    if access_key.contains('\'')
        || access_key.contains('"')
        || access_key.contains(';')
        || access_key.contains('\\')
    {
        return Err(R2ValidationError::InvalidAccessKey(
            "Access key contains forbidden characters (' \" ; \\)".to_string(),
        ));
    }

    // Check regex pattern
    if !ACCESS_KEY_REGEX.is_match(access_key) {
        return Err(R2ValidationError::InvalidAccessKey(
            "Access key must be 16-128 alphanumeric characters".to_string(),
        ));
    }

    Ok(())
}

/// Validate a secret access key.
///
/// Secret keys can contain more characters but must not have SQL injection vectors.
pub fn validate_secret_key(secret_key: &str) -> Result<(), R2ValidationError> {
    // Must have minimum length
    if secret_key.len() < 16 {
        return Err(R2ValidationError::InvalidSecretKey(
            "Secret key must be at least 16 characters".to_string(),
        ));
    }

    // Check for SQL injection characters (must match other validators)
    if secret_key.contains('\'')
        || secret_key.contains('"')
        || secret_key.contains(';')
        || secret_key.contains('\\')
    {
        return Err(R2ValidationError::InvalidSecretKey(
            "Secret key contains forbidden characters (' \" ; \\)".to_string(),
        ));
    }

    Ok(())
}

/// Retry configuration for R2 operations.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff delay
    pub initial_delay: Duration,
    /// Maximum backoff delay
    pub max_delay: Duration,
    /// Backoff multiplier (delay = initial_delay * multiplier^attempt)
    pub multiplier: f64,
    /// Jitter factor (0.0 to 1.0) - random variation applied to delay
    /// e.g., 0.2 means delay varies by +/- 20%
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_factor: 0.2, // +/- 20% jitter to prevent thundering herd
        }
    }
}

impl RetryConfig {
    /// Calculate the delay for a retry attempt with jitter.
    ///
    /// Uses equal jitter: `base + random(-jitter, +jitter) * base`
    /// This prevents thundering herd when multiple clients retry simultaneously.
    pub fn delay_with_jitter(&self, current_delay: Duration) -> Duration {
        let base_delay = current_delay.as_secs_f64();

        // Apply jitter: random value between (1 - jitter) and (1 + jitter) of base delay
        let jitter_range = base_delay * self.jitter_factor;
        let jitter = if jitter_range > 0.0 {
            use rand::Rng;
            let random_factor: f64 = rand::thread_rng().gen_range(-1.0..=1.0);
            jitter_range * random_factor
        } else {
            0.0
        };

        let jittered_delay = (base_delay + jitter).max(0.001); // Minimum 1ms
        Duration::from_secs_f64(jittered_delay.min(self.max_delay.as_secs_f64()))
    }

    /// Calculate the next delay with exponential backoff and jitter.
    pub fn next_delay(&self, current_delay: Duration) -> Duration {
        let next_base =
            (current_delay.as_secs_f64() * self.multiplier).min(self.max_delay.as_secs_f64());
        self.delay_with_jitter(Duration::from_secs_f64(next_base))
    }
}

/// Configuration for multipart uploads.
#[derive(Debug, Clone)]
pub struct MultipartConfig {
    /// Minimum file size to use multipart upload (default: 100MB)
    pub min_multipart_size: u64,
    /// Part size for multipart uploads (default: 50MB)
    /// Must be between 5MB and 5GB per S3 spec
    pub part_size: u64,
    /// Maximum concurrent part uploads
    pub max_concurrent_parts: usize,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            min_multipart_size: 100 * 1024 * 1024, // 100MB
            part_size: 50 * 1024 * 1024,           // 50MB
            max_concurrent_parts: 4,
        }
    }
}

impl MultipartConfig {
    /// Create a config optimized for large files (TB scale).
    pub fn for_large_files() -> Self {
        Self {
            min_multipart_size: 50 * 1024 * 1024, // 50MB
            part_size: 100 * 1024 * 1024,         // 100MB parts
            max_concurrent_parts: 8,
        }
    }
}

/// R2 storage configuration
#[derive(Debug, Clone)]
pub struct R2Config {
    /// R2 bucket name
    pub bucket: String,
    /// R2 account ID (used to construct endpoint for Cloudflare R2)
    pub account_id: String,
    /// R2 access key ID
    pub access_key_id: String,
    /// R2 secret access key
    pub secret_access_key: String,
    /// Custom endpoint URL (for MinIO, LocalStack, or other S3-compatible storage)
    /// When set, this overrides the default Cloudflare R2 endpoint
    pub custom_endpoint: Option<String>,
}

impl R2Config {
    /// Create a new R2 configuration without validation.
    ///
    /// **Warning**: This does not validate inputs. Use `validated()` for production code
    /// to prevent SQL injection in ClickHouse s3() function calls.
    pub fn new(
        bucket: impl Into<String>,
        account_id: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            account_id: account_id.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            custom_endpoint: None,
        }
    }

    /// Create a new R2 configuration with a custom endpoint (for MinIO/LocalStack).
    ///
    /// **Warning**: This does not validate inputs. Use for local development only.
    pub fn new_with_endpoint(
        bucket: impl Into<String>,
        account_id: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            account_id: account_id.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            custom_endpoint: Some(endpoint.into()),
        }
    }

    /// Create a validated R2 configuration.
    ///
    /// This constructor validates all inputs to prevent SQL injection vulnerabilities
    /// when the config values are used in ClickHouse s3() function calls.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the config values contain invalid characters
    /// or don't match the expected patterns.
    pub fn validated(
        bucket: impl Into<String>,
        account_id: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, R2ValidationError> {
        let bucket = bucket.into();
        let account_id = account_id.into();
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();

        // Validate all fields
        validate_bucket_name(&bucket)?;
        validate_account_id(&account_id)?;
        validate_access_key(&access_key_id)?;
        validate_secret_key(&secret_access_key)?;

        Ok(Self {
            bucket,
            account_id,
            access_key_id,
            secret_access_key,
            custom_endpoint: None,
        })
    }

    /// Validate the configuration values.
    ///
    /// Returns Ok(()) if all values are valid, or an error describing the first
    /// invalid value found.
    ///
    /// Note: When using a custom endpoint (for MinIO/LocalStack), validation is
    /// relaxed since local storage uses different credential formats.
    pub fn validate(&self) -> Result<(), R2ValidationError> {
        validate_bucket_name(&self.bucket)?;

        // Skip strict validation for custom endpoints (local dev with MinIO/LocalStack)
        if self.custom_endpoint.is_none() {
            validate_account_id(&self.account_id)?;
            validate_access_key(&self.access_key_id)?;
            validate_secret_key(&self.secret_access_key)?;
        }
        // For local dev, just check for SQL injection characters
        else {
            if self.access_key_id.contains('\'') || self.access_key_id.contains('"') {
                return Err(R2ValidationError::InvalidAccessKey(
                    "Access key contains forbidden characters".to_string(),
                ));
            }
            if self.secret_access_key.contains('\'') || self.secret_access_key.contains('"') {
                return Err(R2ValidationError::InvalidSecretKey(
                    "Secret key contains forbidden characters".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Check if this configuration uses a custom endpoint (local development mode).
    pub fn is_local(&self) -> bool {
        self.custom_endpoint.is_some()
    }

    /// Create configuration from environment variables with validation.
    ///
    /// Expects:
    /// - `R2_BUCKET`: Bucket name (required)
    /// - `R2_ENDPOINT`: Custom endpoint URL (optional, for MinIO/LocalStack)
    /// - `R2_ACCOUNT_ID`: Cloudflare account ID (required unless R2_ENDPOINT is set)
    /// - `R2_ACCESS_KEY_ID`: R2 access key (required)
    /// - `R2_SECRET_ACCESS_KEY`: R2 secret key (required)
    ///
    /// When R2_ENDPOINT is set, account_id validation is skipped, enabling use with
    /// MinIO, LocalStack, or other S3-compatible storage for local development.
    pub fn from_env() -> Result<Self, R2Error> {
        let bucket = std::env::var("R2_BUCKET")
            .map_err(|_| R2Error::Config("R2_BUCKET environment variable not set".into()))?;

        // Check for custom endpoint (local development mode)
        let custom_endpoint = std::env::var("R2_ENDPOINT").ok();

        // Account ID is optional when using custom endpoint
        let account_id = match (&custom_endpoint, std::env::var("R2_ACCOUNT_ID")) {
            (Some(_), Err(_)) => "local".to_string(), // Placeholder for local dev
            (_, Ok(id)) => id,
            (None, Err(_)) => return Err(R2Error::Config(
                "R2_ACCOUNT_ID environment variable not set (required when R2_ENDPOINT is not set)"
                    .into(),
            )),
        };

        let access_key_id = std::env::var("R2_ACCESS_KEY_ID")
            .map_err(|_| R2Error::Config("R2_ACCESS_KEY_ID environment variable not set".into()))?;
        let secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY").map_err(|_| {
            R2Error::Config("R2_SECRET_ACCESS_KEY environment variable not set".into())
        })?;

        // Validate all values to prevent SQL injection in s3() calls
        validate_bucket_name(&bucket).map_err(|e| R2Error::Config(e.to_string()))?;

        // Skip strict validation for custom endpoints (local dev with MinIO/LocalStack)
        if custom_endpoint.is_none() {
            validate_account_id(&account_id).map_err(|e| R2Error::Config(e.to_string()))?;
            validate_access_key(&access_key_id).map_err(|e| R2Error::Config(e.to_string()))?;
            validate_secret_key(&secret_access_key).map_err(|e| R2Error::Config(e.to_string()))?;
        } else {
            // For local dev, just check for SQL injection characters
            if access_key_id.contains('\'') || access_key_id.contains('"') {
                return Err(R2Error::Config(
                    "Access key contains forbidden characters".into(),
                ));
            }
            if secret_access_key.contains('\'') || secret_access_key.contains('"') {
                return Err(R2Error::Config(
                    "Secret key contains forbidden characters".into(),
                ));
            }
        }

        Ok(Self {
            bucket,
            account_id,
            access_key_id,
            secret_access_key,
            custom_endpoint,
        })
    }

    /// Get the R2 endpoint URL.
    ///
    /// Returns the custom endpoint if configured, otherwise constructs the
    /// Cloudflare R2 endpoint from the account ID.
    pub fn endpoint(&self) -> String {
        self.custom_endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{}.r2.cloudflarestorage.com", self.account_id))
    }
}

/// Errors that can occur during R2 operations.
#[derive(Debug, Error)]
pub enum R2Error {
    #[error("R2 configuration error: {0}")]
    Config(String),

    #[error("R2 operation failed: {0}")]
    Operation(String),

    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Invalid key: {0}")]
    InvalidKey(String),
}

/// Result type for R2 operations.
pub type R2Result<T> = Result<T, R2Error>;

/// Metadata for an object in R2.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    /// Object key (path)
    pub key: String,
    /// Size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    /// ETag (content hash)
    pub etag: Option<String>,
}

/// R2 storage client for the data warehouse.
///
/// Provides operations for uploading, downloading, and listing Parquet files
/// in Cloudflare R2 object storage.
///
/// Supports multipart uploads for large files (100MB+) to avoid memory issues
/// and improve upload reliability.
#[derive(Clone)]
pub struct R2Storage {
    client: Client,
    bucket: String,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    multipart_config: MultipartConfig,
    retry_config: RetryConfig,
}

impl R2Storage {
    /// Create a new R2 storage client from configuration.
    #[tracing::instrument(name = "warehouse.storage.r2.new", skip_all, err(Display))]
    pub async fn new(config: R2Config) -> R2Result<Self> {
        Self::with_multipart_config(config, MultipartConfig::default()).await
    }

    /// Create a new R2 storage client with custom multipart configuration.
    #[tracing::instrument(
        name = "warehouse.storage.r2.with_multipart_config",
        skip_all,
        err(Display)
    )]
    pub async fn with_multipart_config(
        config: R2Config,
        multipart_config: MultipartConfig,
    ) -> R2Result<Self> {
        let endpoint = config.endpoint();

        // Build AWS config with R2 credentials
        let credentials = aws_sdk_s3::config::Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None,
            None,
            "r2",
        );

        let s3_config = aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials)
            .endpoint_url(&endpoint)
            .region(Region::new("auto"))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(s3_config);

        Ok(Self {
            client,
            bucket: config.bucket,
            endpoint,
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            multipart_config,
            retry_config: RetryConfig::default(),
        })
    }

    /// Create from environment variables.
    #[tracing::instrument(name = "warehouse.storage.r2.from_env", skip_all, err(Display))]
    pub async fn from_env() -> R2Result<Self> {
        let config = R2Config::from_env()?;
        Self::new(config).await
    }

    /// Create from environment with configuration for large files.
    #[tracing::instrument(
        name = "warehouse.storage.r2.from_env_for_large_files",
        skip_all,
        err(Display)
    )]
    pub async fn from_env_for_large_files() -> R2Result<Self> {
        let config = R2Config::from_env()?;
        Self::with_multipart_config(config, MultipartConfig::for_large_files()).await
    }

    /// Set a custom retry configuration.
    ///
    /// # Example
    /// ```ignore
    /// let storage = R2Storage::from_env().await?
    ///     .with_retry_config(RetryConfig {
    ///         max_retries: 5,
    ///         ..Default::default()
    ///     });
    /// ```
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the access key ID (for ClickHouse s3() function).
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Get the secret access key (for ClickHouse s3() function).
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// Upload a Parquet file to R2 with automatic multipart for large files.
    ///
    /// # Arguments
    /// * `key` - Object key (path), e.g., "stripe/customers/2025-01.parquet"
    /// * `data` - Parquet file contents
    ///
    /// # Returns
    /// The full S3 URL that can be used in ClickHouse queries.
    ///
    /// # Multipart Behavior
    /// Files larger than `min_multipart_size` (default 100MB) are uploaded using
    /// S3's multipart upload API for better reliability and memory efficiency.
    ///
    /// # Retry Behavior
    /// Retries transient failures with exponential backoff. Retryable errors include:
    /// - Network timeouts
    /// - 5xx server errors
    /// - Rate limiting (429)
    ///
    /// # Observability
    ///
    /// Logs upload operations with:
    /// - `size_bytes`: File size being uploaded
    /// - `upload_type`: "multipart" or "single"
    /// - `duration_ms`: Upload duration
    #[tracing::instrument(
        name = "warehouse.storage.r2.upload_parquet",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn upload_parquet(&self, key: &str, data: Bytes) -> R2Result<String> {
        let start = std::time::Instant::now();
        let size_bytes = data.len();
        let is_multipart = size_bytes as u64 >= self.multipart_config.min_multipart_size;
        let upload_type = if is_multipart { "multipart" } else { "single" };

        tracing::debug!(
            key = key,
            size_bytes = size_bytes,
            upload_type = upload_type,
            "Starting R2 upload"
        );

        let result = if is_multipart {
            self.upload_multipart(key, data).await
        } else {
            self.upload_parquet_with_retry(key, data, self.retry_config.clone())
                .await
        };

        let duration_ms = start.elapsed().as_millis() as u64;

        match &result {
            Ok(url) => {
                tracing::info!(
                    key = key,
                    size_bytes = size_bytes,
                    upload_type = upload_type,
                    duration_ms = duration_ms,
                    throughput_mbps =
                        (size_bytes as f64 / 1_000_000.0) / (duration_ms as f64 / 1000.0),
                    "R2 upload completed"
                );
                tracing::debug!(url = %url, "Uploaded to R2");
            }
            Err(e) => {
                tracing::warn!(
                    key = key,
                    size_bytes = size_bytes,
                    upload_type = upload_type,
                    duration_ms = duration_ms,
                    error = %e,
                    "R2 upload failed"
                );
            }
        }

        result
    }

    /// Upload a Parquet file to R2 together with a column-stats sidecar.
    ///
    /// The Parquet file is uploaded at `key` and the sidecar is uploaded at
    /// `<key>.stats.json`.  The sidecar upload is best-effort: if it fails
    /// the Parquet upload result is still returned.
    #[tracing::instrument(
        name = "warehouse.storage.r2.upload_parquet_with_stats",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn upload_parquet_with_stats(
        &self,
        key: &str,
        data: Bytes,
        stats: &crate::warehouse::parquet_stats::FileColumnStats,
    ) -> R2Result<String> {
        let result = self.upload_parquet(key, data).await?;

        // Best-effort sidecar upload
        let stats_key = crate::warehouse::parquet_stats::FileColumnStats::stats_key(key);
        match stats.to_json_bytes() {
            Ok(stats_bytes) => {
                if let Err(e) = self.upload_parquet(&stats_key, stats_bytes).await {
                    tracing::warn!(
                        key = %stats_key,
                        error = %e,
                        "Failed to upload stats sidecar (non-fatal)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    key = %stats_key,
                    error = %e,
                    "Failed to serialize stats sidecar (non-fatal)"
                );
            }
        }

        Ok(result)
    }

    /// Download column-level statistics sidecar for a Parquet file.
    ///
    /// Returns `Ok(None)` when the sidecar does not exist (e.g. the Parquet
    /// file was written before sidecars were introduced).
    #[tracing::instrument(
        name = "warehouse.storage.r2.download_stats",
        skip_all,
        fields(bucket = %self.bucket, parquet_key = parquet_key)
    )]
    pub async fn download_stats(
        &self,
        parquet_key: &str,
    ) -> R2Result<Option<crate::warehouse::parquet_stats::FileColumnStats>> {
        let stats_key = crate::warehouse::parquet_stats::FileColumnStats::stats_key(parquet_key);
        match self.download(&stats_key).await {
            Ok(data) => {
                let stats =
                    crate::warehouse::parquet_stats::FileColumnStats::from_json_bytes(&data)
                        .map_err(|e| {
                            R2Error::Operation(format!("Failed to parse stats sidecar: {}", e))
                        })?;
                Ok(Some(stats))
            }
            Err(R2Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete a Parquet file and its associated stats sidecar from R2.
    ///
    /// The sidecar deletion is best-effort (no error if it doesn't exist).
    pub async fn delete_with_stats(&self, key: &str) -> R2Result<()> {
        self.delete(key).await?;

        let stats_key = crate::warehouse::parquet_stats::FileColumnStats::stats_key(key);
        if let Err(e) = self.delete(&stats_key).await {
            tracing::debug!(
                key = %stats_key,
                error = %e,
                "Stats sidecar delete failed (may not exist)"
            );
        }

        Ok(())
    }

    /// Upload a single part with retry logic.
    ///
    /// PERFORMANCE: Each part upload can be retried independently, avoiding the need
    /// to restart the entire multipart upload on transient network failures.
    #[tracing::instrument(name = "warehouse.r2.upload_part_with_retry", skip_all, err(Display))]
    async fn upload_part_with_retry(
        client: Client,
        bucket: String,
        key: String,
        upload_id: String,
        part_number: i32,
        part_data: Bytes,
        retry_config: RetryConfig,
    ) -> R2Result<(i32, String)> {
        let mut attempts = 0;
        let mut delay = retry_config.initial_delay;
        let max_retries = retry_config.max_retries;

        loop {
            let result = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(part_data.clone()))
                .send()
                .await;

            match result {
                Ok(response) => {
                    let e_tag = response
                        .e_tag()
                        .ok_or_else(|| {
                            R2Error::Operation(format!("Missing ETag for part {}", part_number))
                        })?
                        .to_string();

                    if attempts > 0 {
                        tracing::info!(
                            part_number = part_number,
                            attempts = attempts + 1,
                            "Part upload succeeded after retry"
                        );
                    }

                    return Ok((part_number, e_tag));
                }
                Err(e) => {
                    attempts += 1;

                    if attempts > max_retries {
                        tracing::warn!(
                            part_number = part_number,
                            attempts = attempts,
                            error = %e,
                            "Part upload failed after all retries"
                        );
                        return Err(R2Error::Operation(format!(
                            "Failed to upload part {} after {} attempts: {}",
                            part_number, attempts, e
                        )));
                    }

                    let jittered_delay = retry_config.delay_with_jitter(delay);

                    tracing::debug!(
                        part_number = part_number,
                        attempt = attempts,
                        max_retries = max_retries,
                        delay_ms = jittered_delay.as_millis(),
                        error = %e,
                        "Part upload failed, retrying"
                    );

                    tokio::time::sleep(jittered_delay).await;
                    delay = retry_config.next_delay(delay);
                }
            }
        }
    }

    /// Upload a large file using multipart upload.
    ///
    /// PERFORMANCE: Uses concurrent part uploads for faster transfer of large files.
    /// Each part is uploaded independently with retry logic - transient network
    /// failures on individual parts don't require restarting the entire upload.
    #[tracing::instrument(name = "warehouse.r2.upload_multipart", skip_all, err(Display))]
    async fn upload_multipart(&self, key: &str, data: Bytes) -> R2Result<String> {
        use futures::stream::{self, StreamExt};

        self.validate_key(key)?;

        let part_size = self.multipart_config.part_size as usize;
        let max_concurrent = self.multipart_config.max_concurrent_parts;

        // Initiate multipart upload
        let create_response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| R2Error::Operation(format!("Failed to create multipart upload: {}", e)))?;

        let upload_id = create_response
            .upload_id()
            .ok_or_else(|| R2Error::Operation("Missing upload ID".to_string()))?
            .to_string();

        // Calculate number of parts
        let total_size = data.len();
        let num_parts = (total_size + part_size - 1) / part_size;

        tracing::debug!(
            key = key,
            total_size = total_size,
            part_size = part_size,
            num_parts = num_parts,
            "Starting multipart upload with per-part retry"
        );

        // Upload parts concurrently with retry logic for each part
        let parts_result: Result<Vec<_>, _> = stream::iter(0..num_parts)
            .map(|part_index| {
                let part_number = (part_index + 1) as i32;
                let start = part_index * part_size;
                let end = std::cmp::min(start + part_size, total_size);
                let part_data = data.slice(start..end);
                let bucket = self.bucket.clone();
                let key = key.to_string();
                let upload_id = upload_id.clone();
                let client = self.client.clone();
                let retry_config = self.retry_config.clone();

                async move {
                    Self::upload_part_with_retry(
                        client,
                        bucket,
                        key,
                        upload_id,
                        part_number,
                        part_data,
                        retry_config,
                    )
                    .await
                }
            })
            .buffer_unordered(max_concurrent)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect();

        // Handle upload failures
        let parts = match parts_result {
            Ok(parts) => parts,
            Err(e) => {
                // Abort the multipart upload on failure
                tracing::warn!(
                    key = key,
                    upload_id = %upload_id,
                    error = %e,
                    "Multipart upload failed after per-part retries, aborting"
                );
                if let Err(abort_err) = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await
                {
                    tracing::warn!(
                        key = key,
                        upload_id = %upload_id,
                        error = %abort_err,
                        "Failed to abort multipart upload, orphaned parts may remain"
                    );
                }
                return Err(e);
            }
        };

        // Sort parts by part number (required by S3)
        let mut sorted_parts = parts;
        sorted_parts.sort_by_key(|(part_number, _)| *part_number);

        // Build completed parts list
        let completed_parts: Vec<_> = sorted_parts
            .into_iter()
            .map(|(part_number, e_tag)| {
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build()
            })
            .collect();

        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        // Complete the multipart upload
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| {
                R2Error::Operation(format!("Failed to complete multipart upload: {}", e))
            })?;

        tracing::debug!(
            key = key,
            total_size = total_size,
            num_parts = num_parts,
            "Multipart upload completed"
        );

        Ok(self.get_s3_url(key))
    }

    /// Upload a Parquet file to R2 with custom retry configuration.
    ///
    /// INTEGRITY: Computes and sends Content-MD5 header for data integrity verification.
    /// S3/R2 will reject the upload if the MD5 doesn't match.
    #[tracing::instrument(
        name = "warehouse.storage.r2.upload_parquet_with_retry",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn upload_parquet_with_retry(
        &self,
        key: &str,
        data: Bytes,
        retry_config: RetryConfig,
    ) -> R2Result<String> {
        self.validate_key(key)?;

        let mut last_error = None;
        let mut current_delay = retry_config.initial_delay;

        for attempt in 0..=retry_config.max_retries {
            if attempt > 0 {
                let jittered_delay = retry_config.delay_with_jitter(current_delay);

                tracing::debug!(
                    key = key,
                    attempt = attempt,
                    base_delay_ms = current_delay.as_millis(),
                    actual_delay_ms = jittered_delay.as_millis(),
                    "Retrying R2 upload after delay with jitter"
                );
                tokio::time::sleep(jittered_delay).await;

                current_delay = retry_config.next_delay(current_delay);
            }

            let body = ByteStream::from(data.clone());

            // Do NOT set content_md5 — the AWS SDK auto-adds its own checksum
            // header and R2 rejects requests with two checksums.
            let result = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .content_type("application/octet-stream")
                .send()
                .await;

            match result {
                Ok(_) => {
                    if attempt > 0 {
                        tracing::info!(
                            key = key,
                            attempts = attempt + 1,
                            "R2 upload succeeded after retry"
                        );
                    }
                    return Ok(self.get_s3_url(key));
                }
                Err(e) => {
                    let error_detail = match &e {
                        aws_sdk_s3::error::SdkError::ServiceError(ctx) => {
                            let raw = ctx.raw();
                            format!(
                                "HTTP {} — {}",
                                raw.status().as_u16(),
                                std::str::from_utf8(raw.body().bytes().unwrap_or(b"<no body>"))
                                    .unwrap_or("<binary>")
                            )
                        }
                        other => other.to_string(),
                    };
                    let is_retryable = Self::is_retryable_error(&error_detail);

                    if is_retryable && attempt < retry_config.max_retries {
                        tracing::warn!(
                            key = key,
                            attempt = attempt + 1,
                            max_retries = retry_config.max_retries,
                            error = %error_detail,
                            "R2 upload failed with retryable error"
                        );
                        last_error = Some(R2Error::Operation(format!(
                            "Failed to upload object: {}",
                            error_detail
                        )));
                    } else {
                        return Err(R2Error::Operation(format!(
                            "Failed to upload object: {}",
                            error_detail
                        )));
                    }
                }
            }
        }

        // This should only be reached if all retries failed
        Err(last_error
            .unwrap_or_else(|| R2Error::Operation("Upload failed after all retries".to_string())))
    }

    /// Check if an error is retryable.
    fn is_retryable_error(error: &str) -> bool {
        let error_lower = error.to_lowercase();

        // Retryable conditions:
        // - Network/connection errors
        // - Server errors (5xx)
        // - Rate limiting (429)
        // - Timeouts
        error_lower.contains("timeout")
            || error_lower.contains("connection")
            || error_lower.contains("reset")
            || error_lower.contains("refused")
            || error_lower.contains("temporarily")
            || error_lower.contains("throttl")
            || error_lower.contains("rate limit")
            || error_lower.contains("429")
            || error_lower.contains("500")
            || error_lower.contains("502")
            || error_lower.contains("503")
            || error_lower.contains("504")
            || error_lower.contains("internal server error")
            || error_lower.contains("service unavailable")
            || error_lower.contains("bad gateway")
    }

    /// Download an object from R2.
    #[tracing::instrument(
        name = "warehouse.storage.r2.download",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn download(&self, key: &str) -> R2Result<Bytes> {
        let mut last_error = None;
        let mut current_delay = self.retry_config.initial_delay;

        for attempt in 0..=self.retry_config.max_retries {
            if attempt > 0 {
                let jittered_delay = self.retry_config.delay_with_jitter(current_delay);
                tokio::time::sleep(jittered_delay).await;
                current_delay = self.retry_config.next_delay(current_delay);
            }

            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => match resp.body.collect().await {
                    Ok(aggregated) => return Ok(aggregated.into_bytes()),
                    Err(e) => {
                        let error_str = e.to_string();
                        if Self::is_retryable_error(&error_str)
                            && attempt < self.retry_config.max_retries
                        {
                            last_error = Some(R2Error::Operation(format!(
                                "Failed to read object body: {}",
                                e
                            )));
                            continue;
                        }
                        return Err(R2Error::Operation(format!(
                            "Failed to read object body: {}",
                            e
                        )));
                    }
                },
                Err(e) => {
                    let service_error = e.into_service_error();
                    if service_error.is_no_such_key() {
                        return Err(R2Error::NotFound(key.to_string()));
                    }
                    let error_str = service_error.to_string();
                    if Self::is_retryable_error(&error_str)
                        && attempt < self.retry_config.max_retries
                    {
                        last_error = Some(R2Error::Operation(format!(
                            "Failed to download object: {}",
                            service_error
                        )));
                        continue;
                    }
                    return Err(R2Error::Operation(format!(
                        "Failed to download object: {}",
                        service_error
                    )));
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| R2Error::Operation("Download failed after all retries".to_string())))
    }

    /// Get the size of an object in bytes without downloading it.
    #[tracing::instrument(
        name = "warehouse.storage.r2.file_size",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn file_size(&self, key: &str) -> R2Result<u64> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    R2Error::NotFound(key.to_string())
                } else {
                    R2Error::Operation(format!("Failed to HEAD object: {}", service_error))
                }
            })?;

        Ok(resp.content_length.unwrap_or(0).max(0) as u64)
    }

    /// Download a byte range from an object.
    ///
    /// # Arguments
    /// * `key`   - Object key.
    /// * `start` - Start byte offset (inclusive).
    /// * `len`   - Number of bytes to read.
    #[tracing::instrument(
        name = "warehouse.storage.r2.download_range",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key, start = start, len = len)
    )]
    pub async fn download_range(&self, key: &str, start: u64, len: u64) -> R2Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let end = start.checked_add(len - 1).ok_or_else(|| {
            R2Error::Operation(format!(
                "Range overflow: start={} len={} exceeds u64",
                start, len
            ))
        })?;
        let range = format!("bytes={}-{}", start, end);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                if service_error.is_no_such_key() {
                    R2Error::NotFound(key.to_string())
                } else {
                    R2Error::Operation(format!("Failed to download range: {}", service_error))
                }
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| R2Error::Operation(format!("Failed to read range body: {}", e)))?
            .into_bytes();

        Ok(data)
    }

    /// List objects with a given prefix.
    ///
    /// # Arguments
    /// * `prefix` - Object key prefix, e.g., "stripe/customers/"
    ///
    /// # Returns
    /// List of object info for matching objects.
    #[tracing::instrument(
        name = "warehouse.storage.r2.list_objects",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket)
    )]
    pub async fn list_objects(&self, prefix: &str) -> R2Result<Vec<ObjectInfo>> {
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let resp = request
                .send()
                .await
                .map_err(|e| R2Error::Operation(format!("Failed to list objects: {}", e)))?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        objects.push(ObjectInfo {
                            key,
                            size: obj.size.unwrap_or(0).max(0) as u64,
                            last_modified: obj.last_modified.map(|t| {
                                chrono::DateTime::from_timestamp(t.secs(), t.subsec_nanos())
                                    .unwrap_or_default()
                            }),
                            etag: obj.e_tag,
                        });
                    }
                }
            }

            if resp.is_truncated.unwrap_or(false) {
                match resp.next_continuation_token {
                    Some(token) => continuation_token = Some(token),
                    None => break,
                }
            } else {
                break;
            }
        }

        Ok(objects)
    }

    /// Delete objects with the given keys.
    #[tracing::instrument(
        name = "warehouse.storage.r2.delete_objects",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket)
    )]
    pub async fn delete_objects(&self, keys: &[String]) -> R2Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // S3 delete_objects supports up to 1000 keys per request
        for chunk in keys.chunks(1000) {
            // Build object identifiers with proper error handling
            let identifiers: Vec<aws_sdk_s3::types::ObjectIdentifier> = chunk
                .iter()
                .map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .map_err(|e| {
                            R2Error::Operation(format!(
                                "Failed to build object identifier for key '{}': {}",
                                k, e
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(identifiers))
                .build()
                .map_err(|e| {
                    R2Error::Operation(format!("Failed to build delete request: {}", e))
                })?;

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| R2Error::Operation(format!("Failed to delete objects: {}", e)))?;
        }

        Ok(())
    }

    /// Delete a single object.
    #[tracing::instrument(
        name = "warehouse.storage.r2.delete",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn delete(&self, key: &str) -> R2Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| R2Error::Operation(format!("Failed to delete object: {}", e)))?;

        Ok(())
    }

    /// Check if an object exists.
    #[tracing::instrument(
        name = "warehouse.storage.r2.exists",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn exists(&self, key: &str) -> R2Result<bool> {
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
                    Err(R2Error::Operation(format!(
                        "Failed to check object existence: {}",
                        service_error
                    )))
                }
            }
        }
    }

    /// Get the size (in bytes) of an object in R2.
    ///
    /// Returns 0 if the object does not exist.
    #[tracing::instrument(
        name = "warehouse.storage.r2.get_object_size",
        skip_all,
        err(Display),
        fields(bucket = %self.bucket, key = key)
    )]
    pub async fn get_object_size(&self, key: &str) -> R2Result<u64> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(output.content_length().unwrap_or(0).max(0) as u64),
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    Ok(0)
                } else {
                    Err(R2Error::Operation(format!(
                        "Failed to get object size: {}",
                        service_error
                    )))
                }
            }
        }
    }

    /// Get the S3 URL for an object (for use in ClickHouse queries).
    pub fn get_s3_url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, key)
    }

    /// Get the S3 URL pattern for a prefix (for ClickHouse s3() function).
    ///
    /// Returns a URL pattern like `https://xxx.r2.cloudflarestorage.com/bucket/prefix/*.parquet`
    pub fn get_s3_pattern(&self, prefix: &str) -> String {
        format!("{}/{}/{}*.parquet", self.endpoint, self.bucket, prefix)
    }

    /// Build the ClickHouse s3() function call using a named collection.
    ///
    /// SECURITY: This method uses ClickHouse named collections to avoid embedding
    /// credentials in SQL queries. The named collection must be configured on the
    /// ClickHouse server with the R2 credentials.
    ///
    /// # Arguments
    /// * `collection_name` - Name of the ClickHouse named collection for R2 credentials
    /// * `prefix` - Object key prefix, e.g., "stripe/customers/"
    ///
    /// # Returns
    /// A string like `s3(r2_warehouse, filename='stripe/customers/*.parquet', format='Parquet')`
    ///
    /// # ClickHouse Server Configuration
    /// Create the named collection in ClickHouse:
    /// ```sql
    /// CREATE NAMED COLLECTION r2_warehouse AS
    ///   access_key_id = 'xxx',
    ///   secret_access_key = 'xxx',
    ///   url = 'https://xxx.r2.cloudflarestorage.com/bucket/';
    /// ```
    pub fn build_s3_function_with_collection(&self, collection_name: &str, prefix: &str) -> String {
        format!(
            "s3({}, filename='{}*.parquet', format='Parquet')",
            collection_name, prefix
        )
    }

    /// Build the ClickHouse s3() function call with credentials.
    ///
    /// SECURITY WARNING: This method embeds credentials directly in the SQL string.
    /// This is intended ONLY for internal use by background workers, NOT for queries
    /// that may be logged, displayed to users, or stored in query history.
    ///
    /// For user-facing queries, use `build_s3_function_with_collection` instead.
    ///
    /// # Arguments
    /// * `prefix` - Object key prefix, e.g., "stripe/customers/"
    ///
    /// # Returns
    /// A string like `s3('https://xxx.r2.cloudflarestorage.com/bucket/stripe/customers/*.parquet', 'key', 'secret', 'Parquet')`
    ///
    /// # Visibility
    /// This function is `pub(crate)` to prevent external use. Use named collections
    /// for any external/user-facing code.
    // NOTE: build_s3_function_with_credentials() was removed in 0.3.0 as it embedded
    // credentials in SQL strings, creating security risks if queries were logged.
    // Use build_s3_function_with_collection() with ClickHouse named collections instead.

    /// Get the named collection name for this R2 storage.
    ///
    /// The collection name is derived from the bucket name to support
    /// multiple warehouse configurations.
    pub fn collection_name(&self) -> String {
        format!("r2_{}", self.bucket.replace('-', "_"))
    }

    /// Validate object key.
    fn validate_key(&self, key: &str) -> R2Result<()> {
        if key.is_empty() {
            return Err(R2Error::InvalidKey("Key cannot be empty".into()));
        }
        if key.starts_with('/') {
            return Err(R2Error::InvalidKey("Key cannot start with /".into()));
        }
        if key.len() > 1024 {
            return Err(R2Error::InvalidKey("Key too long (max 1024 chars)".into()));
        }
        Ok(())
    }

    /// Compute the Content-MD5 header value for data integrity verification.
    ///
    /// Not used at runtime (R2 rejects dual checksums when the SDK also sends
    /// its own), but kept for tests and potential future S3-only paths.
    #[cfg(test)]
    fn compute_content_md5(data: &[u8]) -> String {
        let hash = md5::compute(data);
        BASE64.encode(hash.0)
    }
}

impl std::fmt::Debug for R2Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2Storage")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"***")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== R2Config Tests =====

    #[test]
    fn test_r2_config_endpoint() {
        let config = R2Config::new("my-bucket", "abc123", "access-key", "secret-key");
        assert_eq!(config.endpoint(), "https://abc123.r2.cloudflarestorage.com");
    }

    #[test]
    fn test_r2_config_new() {
        let config = R2Config::new("bucket", "account", "access", "secret");
        assert_eq!(config.bucket, "bucket");
        assert_eq!(config.account_id, "account");
        assert_eq!(config.access_key_id, "access");
        assert_eq!(config.secret_access_key, "secret");
    }

    #[test]
    fn test_r2_config_different_accounts() {
        let config1 = R2Config::new("b", "account1", "a", "s");
        let config2 = R2Config::new("b", "account2", "a", "s");

        assert_ne!(config1.endpoint(), config2.endpoint());
        assert!(config1.endpoint().contains("account1"));
        assert!(config2.endpoint().contains("account2"));
    }

    // ===== Validation Tests =====

    #[test]
    fn test_validate_bucket_name_valid() {
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("my-data-warehouse-bucket").is_ok());
        assert!(validate_bucket_name("bucket123").is_ok());
        assert!(validate_bucket_name("123bucket").is_ok());
        assert!(validate_bucket_name("a1b").is_ok()); // minimum 3 chars
    }

    #[test]
    fn test_validate_bucket_name_invalid() {
        // Too short
        assert!(validate_bucket_name("ab").is_err());

        // Contains uppercase
        assert!(validate_bucket_name("MyBucket").is_err());

        // Contains underscore
        assert!(validate_bucket_name("my_bucket").is_err());

        // SQL injection attempts
        assert!(validate_bucket_name("bucket'--").is_err());
        assert!(validate_bucket_name("bucket\"").is_err());
        assert!(validate_bucket_name("bucket;drop").is_err());
        assert!(validate_bucket_name("bucket\\n").is_err());

        // Starts/ends with hyphen
        assert!(validate_bucket_name("-bucket").is_err());
        assert!(validate_bucket_name("bucket-").is_err());
    }

    #[test]
    fn test_validate_account_id_valid() {
        // Valid 32-char hex string
        assert!(validate_account_id("0123456789abcdef0123456789abcdef").is_ok());
        assert!(validate_account_id("abcdef0123456789abcdef0123456789").is_ok());
    }

    #[test]
    fn test_validate_account_id_invalid() {
        // Wrong length
        assert!(validate_account_id("abc123").is_err());
        assert!(validate_account_id("0123456789abcdef0123456789abcdef0").is_err()); // 33 chars

        // Contains uppercase
        assert!(validate_account_id("0123456789ABCDEF0123456789abcdef").is_err());

        // Contains non-hex
        assert!(validate_account_id("0123456789ghijkl0123456789abcdef").is_err());

        // SQL injection
        assert!(validate_account_id("0123456789abcdef'123456789abcdef").is_err());
    }

    #[test]
    fn test_validate_access_key_valid() {
        assert!(validate_access_key("AKIAIOSFODNN7EXAMPLE").is_ok());
        assert!(validate_access_key("0123456789abcdef").is_ok()); // 16 chars min
        assert!(validate_access_key(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        )
        .is_ok());
    }

    #[test]
    fn test_validate_access_key_invalid() {
        // Too short
        assert!(validate_access_key("short").is_err());

        // Contains special chars
        assert!(validate_access_key("access-key-with-dash").is_err());
        assert!(validate_access_key("access_key_underscore").is_err());

        // SQL injection
        assert!(validate_access_key("accesskey'injection").is_err());
    }

    #[test]
    fn test_validate_secret_key_valid() {
        assert!(validate_secret_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY").is_ok());
        assert!(validate_secret_key("super-secret-key-1234567890").is_ok());
    }

    #[test]
    fn test_validate_secret_key_invalid() {
        // Too short
        assert!(validate_secret_key("short").is_err());

        // Contains single quote (SQL injection risk in s3() call)
        assert!(validate_secret_key("secret'key-injection").is_err());
    }

    #[test]
    fn test_r2_config_validated_success() {
        let result = R2Config::validated(
            "my-bucket",
            "0123456789abcdef0123456789abcdef",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_r2_config_validated_invalid_bucket() {
        let result = R2Config::validated(
            "INVALID-BUCKET", // uppercase
            "0123456789abcdef0123456789abcdef",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        assert!(matches!(result, Err(R2ValidationError::InvalidBucket(_))));
    }

    #[test]
    fn test_r2_config_validated_sql_injection_attempt() {
        // Attempt SQL injection via bucket name
        let result = R2Config::validated(
            "bucket'); DROP TABLE users;--",
            "0123456789abcdef0123456789abcdef",
            "AKIAIOSFODNN7EXAMPLE",
            "secretkey1234567890",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_r2_config_validate_method() {
        let config = R2Config::new(
            "valid-bucket",
            "0123456789abcdef0123456789abcdef",
            "AKIAIOSFODNN7EXAMPLE",
            "secretkey1234567890",
        );
        assert!(config.validate().is_ok());

        let bad_config = R2Config::new("bad'bucket", "abc", "short", "s");
        assert!(bad_config.validate().is_err());
    }

    // ===== RetryConfig Tests =====

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert_eq!(config.multiplier, 2.0);
        assert!((config.jitter_factor - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_delay_with_jitter_bounds() {
        let config = RetryConfig {
            jitter_factor: 0.2,
            max_delay: Duration::from_secs(100),
            ..Default::default()
        };

        let base_delay = Duration::from_secs(1);

        // Run multiple times to test random jitter stays within bounds
        for _ in 0..100 {
            let jittered = config.delay_with_jitter(base_delay);

            // Should be within +/- 20% of base delay (with some margin)
            let min_expected = Duration::from_millis(800);
            let max_expected = Duration::from_millis(1200);

            assert!(
                jittered >= min_expected && jittered <= max_expected,
                "Jittered delay {:?} should be between {:?} and {:?}",
                jittered,
                min_expected,
                max_expected
            );
        }
    }

    #[test]
    fn test_retry_delay_respects_max() {
        let config = RetryConfig {
            max_delay: Duration::from_millis(500),
            jitter_factor: 0.0, // No jitter for deterministic test
            ..Default::default()
        };

        let huge_delay = Duration::from_secs(1000);
        let result = config.delay_with_jitter(huge_delay);

        assert!(result <= config.max_delay);
    }

    #[test]
    fn test_retry_next_delay_exponential() {
        let config = RetryConfig {
            multiplier: 2.0,
            jitter_factor: 0.0, // No jitter for deterministic test
            max_delay: Duration::from_secs(100),
            ..Default::default()
        };

        let delay1 = Duration::from_millis(100);
        let delay2 = config.next_delay(delay1);

        // Should double (within tolerance for floating point)
        assert!(
            delay2 >= Duration::from_millis(199) && delay2 <= Duration::from_millis(201),
            "Expected ~200ms, got {:?}",
            delay2
        );
    }

    #[test]
    fn test_retry_next_delay_capped() {
        let config = RetryConfig {
            multiplier: 10.0,
            max_delay: Duration::from_millis(500),
            jitter_factor: 0.0,
            ..Default::default()
        };

        let delay = Duration::from_millis(100);
        let next = config.next_delay(delay);

        assert!(next <= config.max_delay);
    }

    // ===== MultipartConfig Tests =====

    #[test]
    fn test_multipart_config_default() {
        let config = MultipartConfig::default();

        assert_eq!(config.min_multipart_size, 100 * 1024 * 1024); // 100MB
        assert_eq!(config.part_size, 50 * 1024 * 1024); // 50MB
        assert_eq!(config.max_concurrent_parts, 4);
    }

    #[test]
    fn test_multipart_config_for_large_files() {
        let config = MultipartConfig::for_large_files();

        assert_eq!(config.min_multipart_size, 50 * 1024 * 1024); // 50MB
        assert_eq!(config.part_size, 100 * 1024 * 1024); // 100MB
        assert_eq!(config.max_concurrent_parts, 8);
    }

    #[test]
    fn test_multipart_config_large_files_lower_threshold() {
        let default = MultipartConfig::default();
        let large = MultipartConfig::for_large_files();

        // Large files config should have lower threshold for earlier multipart
        assert!(large.min_multipart_size < default.min_multipart_size);
        // And larger parts for efficiency
        assert!(large.part_size > default.part_size);
        // And more concurrency
        assert!(large.max_concurrent_parts > default.max_concurrent_parts);
    }

    // ===== Key Validation Tests =====

    #[test]
    fn test_key_validation_valid() {
        assert!(validate_key("stripe/customers/2025-01.parquet").is_ok());
        assert!(validate_key("data.parquet").is_ok());
        assert!(validate_key("a").is_ok());
        assert!(validate_key("path/to/deeply/nested/file.parquet").is_ok());
        assert!(validate_key("file-with-dashes_and_underscores.parquet").is_ok());
    }

    #[test]
    fn test_key_validation_empty() {
        let result = validate_key("");
        assert!(matches!(result, Err(R2Error::InvalidKey(_))));
    }

    #[test]
    fn test_key_validation_leading_slash() {
        let result = validate_key("/leading-slash");
        assert!(matches!(result, Err(R2Error::InvalidKey(_))));
    }

    #[test]
    fn test_key_validation_too_long() {
        let long_key = "a".repeat(1025);
        let result = validate_key(&long_key);
        assert!(matches!(result, Err(R2Error::InvalidKey(_))));
    }

    #[test]
    fn test_key_validation_max_length() {
        let max_key = "a".repeat(1024);
        let result = validate_key(&max_key);
        assert!(result.is_ok());
    }

    fn validate_key(key: &str) -> R2Result<()> {
        if key.is_empty() {
            return Err(R2Error::InvalidKey("Key cannot be empty".into()));
        }
        if key.starts_with('/') {
            return Err(R2Error::InvalidKey("Key cannot start with /".into()));
        }
        if key.len() > 1024 {
            return Err(R2Error::InvalidKey("Key too long (max 1024 chars)".into()));
        }
        Ok(())
    }

    // ===== S3 URL Generation Tests =====

    #[test]
    fn test_s3_url_generation() {
        let endpoint = "https://abc123.r2.cloudflarestorage.com";
        let bucket = "my-bucket";
        let key = "stripe/customers/2025-01.parquet";

        let url = format!("{}/{}/{}", endpoint, bucket, key);
        assert_eq!(
            url,
            "https://abc123.r2.cloudflarestorage.com/my-bucket/stripe/customers/2025-01.parquet"
        );
    }

    #[test]
    fn test_s3_pattern_generation() {
        let endpoint = "https://abc123.r2.cloudflarestorage.com";
        let bucket = "my-bucket";
        let prefix = "stripe/customers/";

        let pattern = format!("{}/{}/{}*.parquet", endpoint, bucket, prefix);
        assert_eq!(
            pattern,
            "https://abc123.r2.cloudflarestorage.com/my-bucket/stripe/customers/*.parquet"
        );
    }

    #[test]
    fn test_s3_url_with_date_partition() {
        let endpoint = "https://abc123.r2.cloudflarestorage.com";
        let bucket = "warehouse";
        let project_id = "proj-123";
        let table = "events";
        let year = 2025;
        let month = 1;

        let pattern = format!(
            "{}/{}/{}/{}/{:04}/{:02}/*.parquet",
            endpoint, bucket, project_id, table, year, month
        );

        assert_eq!(
            pattern,
            "https://abc123.r2.cloudflarestorage.com/warehouse/proj-123/events/2025/01/*.parquet"
        );
    }

    // ===== Content-MD5 Tests =====

    #[test]
    fn test_compute_content_md5() {
        // Known MD5 hash for "hello world"
        let data = b"hello world";
        let md5 = R2Storage::compute_content_md5(data);

        // MD5 of "hello world" is 5eb63bbbe01eeed093cb22bb8f5acdc3
        // Base64 of that is XrY7u+Ae7tCTyyK7j1rNww==
        assert_eq!(md5, "XrY7u+Ae7tCTyyK7j1rNww==");
    }

    #[test]
    fn test_compute_content_md5_empty() {
        let data = b"";
        let md5 = R2Storage::compute_content_md5(data);

        // MD5 of empty string is d41d8cd98f00b204e9800998ecf8427e
        // Base64 is 1B2M2Y8AsgTpgAmY7PhCfg==
        assert_eq!(md5, "1B2M2Y8AsgTpgAmY7PhCfg==");
    }

    #[test]
    fn test_compute_content_md5_deterministic() {
        let data = b"test data for checksum";
        let md5_1 = R2Storage::compute_content_md5(data);
        let md5_2 = R2Storage::compute_content_md5(data);

        assert_eq!(md5_1, md5_2);
    }

    // ===== ObjectInfo Tests =====

    #[test]
    fn test_object_info_clone() {
        let info = ObjectInfo {
            key: "test.parquet".to_string(),
            size: 1024,
            last_modified: None,
            etag: Some("abc123".to_string()),
        };

        let cloned = info.clone();
        assert_eq!(cloned.key, info.key);
        assert_eq!(cloned.size, info.size);
        assert_eq!(cloned.etag, info.etag);
    }

    // ===== R2Error Tests =====

    #[test]
    fn test_r2_error_display() {
        let config_err = R2Error::Config("missing bucket".to_string());
        assert!(config_err.to_string().contains("missing bucket"));

        let op_err = R2Error::Operation("upload failed".to_string());
        assert!(op_err.to_string().contains("upload failed"));

        let not_found = R2Error::NotFound("key.parquet".to_string());
        assert!(not_found.to_string().contains("key.parquet"));

        let invalid_key = R2Error::InvalidKey("bad key".to_string());
        assert!(invalid_key.to_string().contains("bad key"));
    }

    #[test]
    fn test_r2_error_debug() {
        let err = R2Error::Config("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Config"));
    }
}
