use std::env;
use tracing;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub clickhouse_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    /// JWT issuer claim (iss). Defaults to "reiver".
    pub jwt_issuer: String,
    /// JWT token expiration in hours.
    /// Default: 24 hours. For high-security environments, consider 1-4 hours.
    /// Minimum: 1 hour. Maximum: 168 hours (7 days).
    pub jwt_expiration_hours: i64,
    // Kafka configuration
    pub kafka_hosts: String,
    /// Kafka hosts for ClickHouse (inside Docker, needs internal address like redpanda:9092)
    pub clickhouse_kafka_hosts: String,
    pub kafka_exceptions_topic: String,
    pub kafka_spans_topic: String,
    pub kafka_logs_otlp_topic: String,
    pub kafka_logs_unstructured_topic: String,
    pub kafka_llm_chunks_topic: String,
    pub kafka_metrics_topic: String,
    pub kafka_sync_jobs_topic: String,
    pub kafka_pipeline_events_topic: String,
    pub kafka_platform_events_topic: String,
    pub kafka_session_eval_jobs_topic: String,
    pub kafka_client_id: Option<String>,
    pub kafka_producer_linger_ms: i32,
    pub kafka_producer_max_retries: i32,
    pub kafka_message_timeout_ms: i32,
    pub kafka_socket_timeout_ms: i32,
    pub kafka_compression_codec: String,
    pub kafka_acks: String,

    // CORS configuration
    /// Comma-separated list of allowed origins (e.g., "https://app.example.com,https://admin.example.com")
    /// Use "*" for development to allow all origins (not recommended for production)
    pub cors_allowed_origins: Vec<String>,
    /// Whether to allow credentials in CORS requests
    pub cors_allow_credentials: bool,

    // Encryption configuration
    /// Base64-encoded 32-byte AES-256 encryption key for secrets
    /// Generate with: openssl rand -base64 32
    /// If not set, a random key is generated (development only - secrets won't persist across restarts!)
    pub encryption_key: Option<String>,

    // Query limits
    /// Maximum number of rows to return from ClickHouse queries
    /// Default: 10000, Max: 100000
    pub clickhouse_max_rows: u32,
    /// Default number of rows for paginated queries
    /// Default: 100
    pub clickhouse_default_limit: u32,

    // Rate limiting
    /// Rate limit for analytics endpoints (per minute)
    /// Default: 240
    pub rate_limit_analytics_per_minute: i32,
    /// Rate limit for analytics endpoints (per hour)
    /// Default: 1200
    pub rate_limit_analytics_per_hour: i32,
    /// Rate limit for CRUD endpoints (per minute)
    /// Default: 480
    pub rate_limit_crud_per_minute: i32,
    /// Rate limit for CRUD endpoints (per hour)
    /// Default: 4800
    pub rate_limit_crud_per_hour: i32,
    /// Rate limit for billing/usage endpoints (per minute)
    /// These endpoints query ClickHouse and can be expensive.
    /// Default: 30 (more restrictive than analytics)
    pub rate_limit_billing_per_minute: i32,
    /// Rate limit for billing/usage endpoints (per hour)
    /// Default: 120
    pub rate_limit_billing_per_hour: i32,
    /// Rate limit for AI Gateway endpoints (per minute per project)
    /// Prevents cost attacks and provider rate limit exhaustion.
    /// Default: 300
    pub rate_limit_gateway_per_minute: i32,
    /// Rate limit for AI Gateway endpoints (per hour per project)
    /// Default: 10000
    pub rate_limit_gateway_per_hour: i32,
    /// Rate limit for external API calls (per minute per user)
    /// Used for endpoints that call external services like GitHub API.
    /// More restrictive to prevent exhausting third-party rate limits.
    /// Default: 30
    pub rate_limit_external_api_per_minute: i32,
    /// Rate limit for external API calls (per hour per user)
    /// Default: 300
    pub rate_limit_external_api_per_hour: i32,
    /// Rate limit for NL (Text-to-SQL) queries (per minute per project).
    /// More restrictive because each NL query triggers up to 3 LLM calls
    /// plus 3 ClickHouse queries.
    /// Default: 10
    pub rate_limit_nl_query_per_minute: i32,
    /// Rate limit for NL (Text-to-SQL) queries (per hour per project)
    /// Default: 60
    pub rate_limit_nl_query_per_hour: i32,
    /// Rate limit for telemetry ingestion (per minute per project)
    /// Higher than other limits since telemetry is expected to be high volume.
    /// Default: 1000
    pub rate_limit_ingestion_per_minute: i32,
    /// Rate limit for telemetry ingestion (per hour per project)
    /// Default: 30000
    pub rate_limit_ingestion_per_hour: i32,

    // Cookie configuration
    /// Cookie domain for authentication cookies.
    /// If set, cookies will be scoped to this domain (e.g., ".example.com" for all subdomains).
    /// If not set, the cookie will use the current host (default browser behavior).
    /// SECURITY: In production, set this to your exact domain to prevent cookie leakage.
    pub cookie_domain: Option<String>,

    // SAML configuration
    /// SAML assertion time skew tolerance in seconds.
    /// Allows for clock differences between SP and IdP.
    /// Default: 60 seconds. Enterprise environments with clock sync issues may need more.
    /// Maximum: 300 seconds (5 minutes) to prevent excessive replay window.
    pub saml_time_skew_seconds: i64,

    // MFA configuration
    /// TOTP algorithm for MFA (sha1 or sha256).
    /// Default: sha1 (RFC 6238 default, widely compatible)
    /// sha256 provides stronger security for compliance-focused organizations.
    /// Note: Most authenticator apps support both, but verify compatibility before changing.
    pub totp_algorithm: TotpAlgorithm,

    /// MFA challenge time-to-live in seconds.
    /// Default: 180 (3 minutes).
    /// Increase for users with accessibility needs.
    pub mfa_challenge_ttl_seconds: i64,

    // Session security configuration
    /// Enable IP binding for SSO sessions.
    /// When enabled, sessions are bound to the client IP address that created them.
    /// If the IP changes, the session is invalidated.
    /// Default: false (disabled for better UX with mobile/roaming users)
    /// SECURITY: Enable for high-security environments, but may cause issues with VPNs.
    pub session_ip_binding_enabled: bool,

    /// Enable user agent binding for SSO sessions.
    /// When enabled, sessions are bound to the browser user-agent string.
    /// If the user-agent changes, the session is invalidated.
    /// Default: false (disabled, as user-agents can change with browser updates)
    /// SECURITY: Provides additional session security but may cause user friction.
    pub session_user_agent_binding_enabled: bool,

    /// Base URL for the application (used for SSO callback URLs, etc.)
    /// SECURITY: In production, this MUST use HTTPS to prevent credential interception.
    /// Default: http://localhost:3000 (for development only)
    pub base_url: String,

    /// Allow new user registration (signup). When false, only pre-seeded or SSO users can log in.
    pub allow_signup: bool,

    /// Allow email/password login. When false, only OAuth and SSO login are available.
    pub allow_password_login: bool,

    // Stripe configuration
    /// Stripe API secret key (sk_test_... or sk_live_...)
    pub stripe_api_key: Option<String>,
    /// Stripe webhook signing secret (whsec_...)
    pub stripe_webhook_secret: Option<String>,
    /// Allowed Stripe price IDs (comma-separated)
    /// Only these price IDs can be used when creating subscriptions.
    /// SECURITY: If empty, subscription creation is REJECTED (fail-closed).
    /// This prevents accidental subscription to unintended Stripe plans.
    pub stripe_allowed_price_ids: Vec<String>,
    /// Stripe metered price ID for platform usage billing.
    /// This price is automatically attached as an additional subscription item
    /// when creating subscriptions. Stripe Meters report usage against this price
    /// and Stripe invoices the aggregated amount each billing cycle.
    /// Created via Stripe Dashboard or API: $0.01/unit (1 unit = 1 cent).
    pub stripe_metered_price_id: Option<String>,

    /// Enable Stripe webhook IP allowlisting (defense-in-depth)
    /// When enabled, webhook requests are validated against known Stripe IP ranges.
    /// Default: false (rely on signature verification only)
    pub stripe_webhook_ip_allowlist_enabled: bool,

    /// Stripe webhook IP allowlist (CIDR notation, comma-separated)
    /// Only used when stripe_webhook_ip_allowlist_enabled is true.
    /// Example: "3.18.12.63/32,3.130.192.231/32,13.235.14.237/32"
    /// See: https://docs.stripe.com/webhooks#verify-official-stripe-ip-addresses
    pub stripe_webhook_ip_allowlist: Vec<String>,

    /// URL to redirect back to after the user finishes in the Stripe Customer Portal.
    /// Defaults to "/settings/billing".
    pub stripe_portal_return_url: String,

    // Billing configuration
    /// Enable the credit system and platform-managed API keys.
    /// When false, only BYOK (user-provided) keys are available and all
    /// credit-related UI, balance checks, and deductions are disabled.
    /// Default: false (BYOK-only mode for initial launch)
    pub credits_enabled: bool,

    /// Cooldown period (in hours) between sending the same type of budget alert.
    /// This prevents alert fatigue from repeated notifications.
    /// Default: 24 hours
    pub budget_alert_cooldown_hours: i64,

    // AI Gateway configuration
    /// Enable automatic fallback to alternative LLM providers when the primary fails.
    /// When a provider returns rate limit or server errors, the gateway will try
    /// fallback models (e.g., claude -> gpt-4 -> gemini).
    /// Default: true
    pub gateway_fallback_enabled: bool,

    /// Maximum retry attempts per provider before trying fallback.
    /// Uses exponential backoff between retries.
    /// Default: 2
    pub gateway_max_retries: u32,

    /// Initial retry delay in milliseconds.
    /// Doubles with each retry attempt (exponential backoff).
    /// Default: 500ms
    pub gateway_initial_retry_delay_ms: u64,

    /// Maximum retry delay in milliseconds.
    /// Caps the exponential backoff.
    /// Default: 10000ms (10 seconds)
    pub gateway_max_retry_delay_ms: u64,

    /// Enable semantic caching for LLM requests (requires semcache service).
    /// Default: false
    pub gateway_cache_enabled: bool,

    /// Semcache service URL for semantic caching.
    /// Default: http://localhost:8080
    pub gateway_cache_url: String,

    /// Cache TTL in seconds for LLM responses.
    /// Default: 86400 (24 hours)
    pub gateway_cache_ttl_seconds: u64,

    /// Enable logging of request/response content in gateway observability.
    /// When disabled, request messages and response content are not stored.
    /// Default: false (disabled for privacy/PII protection)
    /// SECURITY: Enable with caution - may store PII in ClickHouse.
    pub gateway_log_content: bool,

    /// Default request timeout in seconds for LLM provider API calls.
    /// This applies to all providers unless overridden by provider-specific settings.
    /// Default: 120 seconds
    pub gateway_timeout_seconds: u64,

    /// Request timeout in seconds specifically for OpenAI API calls.
    /// Overrides gateway_timeout_seconds for OpenAI models.
    /// Default: 120 seconds
    pub gateway_timeout_openai_seconds: u64,

    /// Request timeout in seconds specifically for Anthropic API calls.
    /// Overrides gateway_timeout_seconds for Anthropic models.
    /// Default: 120 seconds
    pub gateway_timeout_anthropic_seconds: u64,

    /// Request timeout in seconds specifically for Google Gemini API calls.
    /// Overrides gateway_timeout_seconds for Gemini models.
    /// Default: 120 seconds
    pub gateway_timeout_google_seconds: u64,

    /// Request timeout in seconds specifically for AWS Bedrock API calls.
    /// Overrides gateway_timeout_seconds for Bedrock models.
    /// Default: 180 seconds (longer for Bedrock due to cold starts)
    pub gateway_timeout_bedrock_seconds: u64,

    /// Anthropic API version header value.
    ///
    /// The Anthropic API requires an `anthropic-version` header. This allows
    /// updating to newer API versions without code changes.
    ///
    /// See: https://docs.anthropic.com/en/api/versioning
    /// Default: "2023-06-01" (stable version)
    pub gateway_anthropic_api_version: String,

    /// Override the OpenAI API base URL.
    /// Primarily used for testing (wiremock) and Azure OpenAI compatibility.
    /// Default: https://api.openai.com/v1
    pub gateway_openai_base_url: Option<String>,

    /// Override the Anthropic API base URL.
    /// Primarily used for testing (wiremock).
    /// Default: https://api.anthropic.com/v1
    pub gateway_anthropic_base_url: Option<String>,

    /// Override the Google Gemini API base URL.
    /// Primarily used for testing (wiremock).
    /// Default: https://generativelanguage.googleapis.com/v1beta
    pub gateway_google_base_url: Option<String>,

    /// Deprecated — the Theta on-demand API base URL is now hardcoded.
    /// Kept only for backward-compatible config parsing; the value is ignored.
    #[allow(dead_code)]
    pub gateway_theta_base_url: Option<String>,

    /// DeepSeek API endpoint base URL.
    /// Default: https://api.deepseek.com
    pub gateway_deepseek_base_url: Option<String>,

    pub gateway_xai_base_url: Option<String>,
    pub gateway_mistral_base_url: Option<String>,
    pub gateway_groq_base_url: Option<String>,
    pub gateway_together_base_url: Option<String>,
    pub gateway_fireworks_base_url: Option<String>,
    pub gateway_perplexity_base_url: Option<String>,
    pub gateway_cohere_base_url: Option<String>,
    pub gateway_openrouter_base_url: Option<String>,
    pub gateway_cerebras_base_url: Option<String>,
    pub gateway_deepinfra_base_url: Option<String>,
    pub gateway_alibaba_base_url: Option<String>,
    pub gateway_nvidia_base_url: Option<String>,
    pub gateway_ai21_base_url: Option<String>,
    pub gateway_sambanova_base_url: Option<String>,
    pub gateway_lambda_base_url: Option<String>,
    pub gateway_lepton_base_url: Option<String>,
    pub gateway_hyperbolic_base_url: Option<String>,
    pub gateway_ovhcloud_base_url: Option<String>,
    pub gateway_novita_base_url: Option<String>,
    pub gateway_huggingface_base_url: Option<String>,
    pub gateway_cloudflare_base_url: Option<String>,
    pub gateway_azure_openai_base_url: Option<String>,
    pub gateway_vertex_ai_base_url: Option<String>,

    /// Request timeout in seconds specifically for Theta EdgeCloud API calls.
    /// Overrides gateway_timeout_seconds for Theta models.
    /// Default: 120 seconds
    pub gateway_timeout_theta_seconds: u64,

    /// Request timeout in seconds specifically for DeepSeek API calls.
    /// Default: 120 seconds
    pub gateway_timeout_deepseek_seconds: u64,

    /// Shared timeout for all OpenAI-compatible wrapper providers.
    /// Default: 120 seconds
    pub gateway_timeout_openai_compat_seconds: u64,

    /// Platform-level fallback API key for OpenAI.
    /// Used when a project has not configured its own OpenAI key.
    /// Set via GATEWAY_DEFAULT_OPENAI_API_KEY env var.
    pub gateway_default_openai_api_key: Option<String>,

    /// Platform-level fallback API key for Anthropic.
    /// Used when a project has not configured its own Anthropic key.
    /// Set via GATEWAY_DEFAULT_ANTHROPIC_API_KEY env var.
    pub gateway_default_anthropic_api_key: Option<String>,

    /// Platform-level fallback API key for Google Gemini.
    /// Used when a project has not configured its own Google key.
    /// Set via GATEWAY_DEFAULT_GOOGLE_API_KEY env var.
    pub gateway_default_google_api_key: Option<String>,

    /// Platform-level fallback API key for Theta EdgeCloud.
    /// Used when a project has not configured its own Theta key.
    /// Set via GATEWAY_DEFAULT_THETA_API_KEY env var.
    pub gateway_default_theta_api_key: Option<String>,

    /// Platform-level fallback API key for DeepSeek.
    /// Used when a project has not configured its own DeepSeek key.
    /// Set via GATEWAY_DEFAULT_DEEPSEEK_API_KEY env var.
    pub gateway_default_deepseek_api_key: Option<String>,

    /// Model to use for LLM-as-judge evaluations in the playground.
    /// This model evaluates response quality (relevance, coherence, helpfulness).
    /// Should be a fast, cost-effective model.
    /// Default: "gpt-4o-mini"
    pub playground_evaluation_model: String,

    // GitHub App integration
    /// GitHub App ID for the Reiver GitHub App.
    /// Get this from your GitHub App settings page.
    pub github_app_id: Option<u64>,

    /// GitHub App name (slug) for installation URLs.
    /// This is the name that appears in the URL: github.com/apps/{name}
    pub github_app_name: Option<String>,

    /// GitHub App private key in PEM format.
    /// Used to authenticate as the GitHub App and generate installation tokens.
    /// Get this from your GitHub App settings page (generate a new private key).
    pub github_app_private_key: Option<String>,

    /// GitHub App webhook secret for verifying webhook payloads.
    /// Used to validate that webhooks are actually from GitHub.
    pub github_app_webhook_secret: Option<String>,

    /// GitHub webhook IP allowlist for defense-in-depth security.
    /// If set, webhooks will be rejected unless they come from one of these CIDR ranges.
    /// Get current ranges from: https://api.github.com/meta (hooks field)
    /// Leave empty to disable IP allowlist (signature verification is still enforced).
    /// Example: "192.30.252.0/22,185.199.108.0/22,140.82.112.0/20,143.55.64.0/20"
    pub github_webhook_ip_allowlist: Vec<String>,

    /// Trusted proxy CIDR ranges for X-Forwarded-For header parsing.
    /// When a webhook request comes from one of these IPs, the X-Forwarded-For header
    /// will be used to determine the real client IP for allowlist checking.
    /// This is required when behind a reverse proxy (nginx, Cloudflare, AWS ALB, etc.).
    /// Leave empty to only trust the direct connection IP (default, most secure).
    /// Example: "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16" for RFC 1918 private networks.
    pub trusted_proxy_cidrs: Vec<String>,

    /// Base URL for the Reiver API (used for GitHub OAuth callback URLs).
    /// Example: https://api.reiver.io
    pub api_base_url: Option<String>,

    // Slack App (OAuth + Events API)
    pub slack_client_id: Option<String>,
    pub slack_client_secret: Option<String>,
    pub slack_signing_secret: Option<String>,

    // Social OAuth login (Google, GitHub, Microsoft)
    pub oauth_google_client_id: Option<String>,
    pub oauth_google_client_secret: Option<String>,
    pub oauth_github_client_id: Option<String>,
    pub oauth_github_client_secret: Option<String>,
    pub oauth_microsoft_client_id: Option<String>,
    pub oauth_microsoft_client_secret: Option<String>,

    // Asset storage configuration
    /// Storage backend for multimodal prompt assets.
    /// Options: "memory" (tests), "local" (development), "s3" (production)
    /// Default: "local"
    pub storage_backend: String,

    /// Local filesystem path for asset storage when using "local" backend.
    /// Default: "./data/assets"
    pub storage_local_path: String,

    /// Base URL for serving local assets.
    /// This is the public URL where assets can be accessed.
    /// Default: "http://localhost:3000/api/assets"
    pub storage_local_base_url: String,

    /// S3 bucket name for asset storage when using "s3" backend.
    pub storage_s3_bucket: Option<String>,

    /// S3 region for asset storage.
    /// Default: "us-east-1"
    pub storage_s3_region: String,

    /// S3 custom endpoint for S3-compatible services (MinIO, R2, etc.)
    pub storage_s3_endpoint: Option<String>,

    /// Use path-style addressing for S3 (required for some S3-compatible services).
    /// Default: false
    pub storage_s3_path_style: bool,

    /// URL for the Flow (LLM Gateway) service.
    /// Used by Pond to call Flow's chat completions API for text-to-SQL.
    /// Default: "http://localhost:3001"
    pub flow_gateway_url: String,

    // OpenTelemetry configuration (dogfooding)
    /// OTLP exporter endpoint for sending traces to Watch.
    /// When set, enables OpenTelemetry trace export alongside console logging.
    /// Example: "http://localhost:3000" (Watch service URL)
    /// If not set, only console logging is used (default behavior).
    pub otel_exporter_endpoint: Option<String>,

    /// Watch project ID for tagging exported telemetry.
    /// This UUID is sent as the X-Project-Id header to Watch's OTLP ingestion endpoint.
    /// Required when otel_exporter_endpoint is set.
    pub otel_project_id: Option<String>,

    // Continuous profiling configuration (CPU + heap)
    /// Enable continuous profiling (opt-in).
    /// Requires otel_exporter_endpoint and otel_project_id to be set.
    /// Default: false
    pub profiling_enabled: bool,

    /// CPU profiling sampling frequency in Hz.
    /// 99 Hz avoids lock-step with common timers.
    /// Default: 99
    pub profiling_frequency: i32,

    /// How often to export a CPU profile snapshot, in seconds.
    /// Default: 600 (10 minutes)
    pub profiling_cpu_interval_secs: u64,

    /// How often to export a heap profile snapshot, in seconds.
    /// Only effective on Linux where jemalloc heap profiling is available.
    /// Default: 600 (10 minutes)
    pub profiling_heap_interval_secs: u64,

    // Loops.so transactional email
    /// Loops API key. When unset, email sending is silently disabled.
    pub loops_api_key: Option<String>,
    /// Transactional template ID for org invitation emails.
    pub loops_invite_template_id: Option<String>,
    /// Transactional template ID for alert notification emails.
    pub loops_alert_template_id: Option<String>,
    /// Transactional template ID for welcome emails on signup.
    pub loops_welcome_template_id: Option<String>,
    /// Public base URL for the app (used in invite links, e.g. https://reiver.ai).
    pub app_url: Option<String>,
}

/// Supported TOTP algorithms
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TotpAlgorithm {
    /// SHA1 - RFC 6238 default, widely compatible
    Sha1,
    /// SHA256 - stronger security, good for compliance
    Sha256,
}

impl TotpAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            TotpAlgorithm::Sha1 => "SHA1",
            TotpAlgorithm::Sha256 => "SHA256",
        }
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let kafka_hosts = env::var("KAFKA_HOSTS").unwrap_or_else(|_| "localhost:9092".to_string());
        tracing::info!("Loading config: KAFKA_HOSTS={}", kafka_hosts);

        let config = Config {
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:postgres@localhost:5432/reiver".to_string()
            }),
            clickhouse_url: env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://default:@localhost:8123".to_string()),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_default(),
            jwt_issuer: env::var("JWT_ISSUER").unwrap_or_else(|_| "reiver".to_string()),
            // JWT expiration - validate within bounds (1-168 hours)
            jwt_expiration_hours: {
                let hours = env::var("JWT_EXPIRATION_HOURS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(24);
                if hours < 1 {
                    tracing::warn!(
                        "JWT_EXPIRATION_HOURS ({}) is below minimum of 1 hour. Using 1 hour.",
                        hours
                    );
                    1
                } else if hours > 168 {
                    tracing::warn!(
                        "JWT_EXPIRATION_HOURS ({}) exceeds maximum of 168 hours (7 days). \
                         Using 168 hours to limit token exposure window.",
                        hours
                    );
                    168
                } else {
                    hours
                }
            },
            // Kafka configuration (PostHog-style defaults)
            kafka_hosts: kafka_hosts.clone(),
            // ClickHouse runs inside Docker and needs the internal Docker network address
            // Default to redpanda:9092 for Docker, can be overridden for other setups
            clickhouse_kafka_hosts: env::var("CLICKHOUSE_KAFKA_HOSTS")
                .unwrap_or_else(|_| "redpanda:9092".to_string()),
            kafka_exceptions_topic: env::var("KAFKA_EXCEPTIONS_TOPIC")
                .unwrap_or_else(|_| "reiver.exceptions".to_string()),
            kafka_spans_topic: env::var("KAFKA_SPANS_TOPIC")
                .unwrap_or_else(|_| "reiver.spans".to_string()),
            kafka_logs_otlp_topic: env::var("KAFKA_LOGS_OTLP_TOPIC")
                .unwrap_or_else(|_| "reiver.logs.otlp".to_string()),
            kafka_logs_unstructured_topic: env::var("KAFKA_LOGS_UNSTRUCTURED_TOPIC")
                .unwrap_or_else(|_| "reiver.logs.unstructured".to_string()),
            kafka_llm_chunks_topic: env::var("KAFKA_LLM_CHUNKS_TOPIC")
                .unwrap_or_else(|_| "reiver.llm.chunks".to_string()),
            kafka_metrics_topic: env::var("KAFKA_METRICS_TOPIC")
                .unwrap_or_else(|_| "reiver.metrics".to_string()),
            kafka_sync_jobs_topic: env::var("KAFKA_SYNC_JOBS_TOPIC")
                .unwrap_or_else(|_| "reiver.warehouse.sync_jobs".to_string()),
            kafka_pipeline_events_topic: env::var("KAFKA_PIPELINE_EVENTS_TOPIC")
                .unwrap_or_else(|_| "reiver.pipeline.events".to_string()),
            kafka_platform_events_topic: env::var("KAFKA_PLATFORM_EVENTS_TOPIC")
                .unwrap_or_else(|_| "reiver.platform.events".to_string()),
            kafka_session_eval_jobs_topic: env::var("KAFKA_SESSION_EVAL_JOBS_TOPIC")
                .unwrap_or_else(|_| "reiver.session.eval.jobs".to_string()),
            kafka_client_id: env::var("KAFKA_CLIENT_ID").ok(),
            kafka_producer_linger_ms: env::var("KAFKA_PRODUCER_LINGER_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5), // 5ms linger for better batching under load
            kafka_producer_max_retries: env::var("KAFKA_PRODUCER_MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            kafka_message_timeout_ms: env::var("KAFKA_MESSAGE_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10000), // 10s timeout
            kafka_socket_timeout_ms: env::var("KAFKA_SOCKET_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60000), // 60s socket timeout
            kafka_compression_codec: env::var("KAFKA_COMPRESSION_CODEC")
                .unwrap_or_else(|_| "snappy".to_string()),
            kafka_acks: env::var("KAFKA_ACKS").unwrap_or_else(|_| "1".to_string()), // Wait for leader ack

            // CORS configuration
            // In production, require explicit configuration; in development, allow all origins
            cors_allowed_origins: {
                let is_production = env::var("ENVIRONMENT")
                    .map(|e| e.to_lowercase() == "production")
                    .unwrap_or(false);

                match env::var("CORS_ALLOWED_ORIGINS") {
                    Ok(origins) => origins
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                    Err(_) if is_production => {
                        tracing::warn!(
                            "SECURITY WARNING: CORS_ALLOWED_ORIGINS not set in production. \
                             Defaulting to empty list (no cross-origin requests allowed). \
                             Set CORS_ALLOWED_ORIGINS to your frontend domain(s)."
                        );
                        Vec::new()
                    }
                    Err(_) => {
                        tracing::warn!(
                            "CORS_ALLOWED_ORIGINS not set. Using '*' for development. \
                             Set explicit origins for production."
                        );
                        vec!["*".to_string()]
                    }
                }
            },
            cors_allow_credentials: env::var("CORS_ALLOW_CREDENTIALS")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(true),

            // Encryption key for secrets (SSO client secrets, API tokens, etc.)
            encryption_key: env::var("ENCRYPTION_KEY").ok(),

            // Query limits - prevent runaway queries
            clickhouse_max_rows: env::var("CLICKHOUSE_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(|v: u32| v.min(100_000)) // Hard cap at 100k
                .unwrap_or(10_000),
            clickhouse_default_limit: env::var("CLICKHOUSE_DEFAULT_LIMIT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),

            // Rate limiting - PostHog-style defaults
            rate_limit_analytics_per_minute: env::var("RATE_LIMIT_ANALYTICS_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(240),
            rate_limit_analytics_per_hour: env::var("RATE_LIMIT_ANALYTICS_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_200),
            rate_limit_crud_per_minute: env::var("RATE_LIMIT_CRUD_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(480),
            rate_limit_crud_per_hour: env::var("RATE_LIMIT_CRUD_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4_800),
            rate_limit_billing_per_minute: env::var("RATE_LIMIT_BILLING_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            rate_limit_billing_per_hour: env::var("RATE_LIMIT_BILLING_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),
            rate_limit_gateway_per_minute: env::var("RATE_LIMIT_GATEWAY_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            rate_limit_gateway_per_hour: env::var("RATE_LIMIT_GATEWAY_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            rate_limit_external_api_per_minute: env::var("RATE_LIMIT_EXTERNAL_API_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            rate_limit_external_api_per_hour: env::var("RATE_LIMIT_EXTERNAL_API_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            rate_limit_nl_query_per_minute: env::var("RATE_LIMIT_NL_QUERY_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            rate_limit_nl_query_per_hour: env::var("RATE_LIMIT_NL_QUERY_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            rate_limit_ingestion_per_minute: env::var("RATE_LIMIT_INGESTION_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            rate_limit_ingestion_per_hour: env::var("RATE_LIMIT_INGESTION_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30000),

            // Cookie configuration
            cookie_domain: env::var("COOKIE_DOMAIN").ok(),

            // SAML configuration - cap at 300 seconds to prevent excessive replay window
            saml_time_skew_seconds: {
                let skew = env::var("SAML_TIME_SKEW_SECONDS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                if skew > 300 {
                    tracing::warn!(
                        "SAML_TIME_SKEW_SECONDS ({}) exceeds maximum of 300 seconds. \
                         Using 300 seconds to prevent excessive assertion replay window.",
                        skew
                    );
                    300
                } else {
                    skew
                }
            },

            // MFA configuration
            totp_algorithm: env::var("TOTP_ALGORITHM")
                .map(|v| match v.to_lowercase().as_str() {
                    "sha256" => TotpAlgorithm::Sha256,
                    "sha1" | _ => TotpAlgorithm::Sha1,
                })
                .unwrap_or(TotpAlgorithm::Sha1),

            mfa_challenge_ttl_seconds: env::var("MFA_CHALLENGE_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180), // Default: 3 minutes

            // Session security configuration
            session_ip_binding_enabled: env::var("SESSION_IP_BINDING_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false), // Disabled by default for UX

            session_user_agent_binding_enabled: env::var("SESSION_USER_AGENT_BINDING_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false), // Disabled by default

            // Base URL configuration
            // SECURITY: Validate HTTPS is used in production to prevent credential interception
            base_url: {
                let is_production = env::var("ENVIRONMENT")
                    .map(|e| e.to_lowercase() == "production")
                    .unwrap_or(false);
                let allow_insecure = env::var("ALLOW_INSECURE_HTTP")
                    .map(|v| v == "true")
                    .unwrap_or(false);

                match env::var("BASE_URL") {
                    Ok(url) => {
                        if is_production && !url.starts_with("https://") && !allow_insecure {
                            tracing::error!(
                                "SECURITY ERROR: BASE_URL must use HTTPS in production. \
                                 Current value: {}. SSO callbacks will fail with HTTP URLs. \
                                 Set BASE_URL to your HTTPS domain (e.g., https://app.example.com)",
                                url
                            );
                            return Err(anyhow::anyhow!(
                                "BASE_URL must use HTTPS in production environments"
                            ));
                        }
                        if is_production && !url.starts_with("https://") && allow_insecure {
                            tracing::warn!(
                                "ALLOW_INSECURE_HTTP is set: accepting HTTP BASE_URL ({}) in production. \
                                 Remove this override once TLS is configured.",
                                url
                            );
                        }
                        if !is_production && !url.starts_with("https://") {
                            tracing::warn!(
                                "SECURITY WARNING: BASE_URL uses HTTP ({}). \
                                 This is acceptable for development but MUST use HTTPS in production.",
                                url
                            );
                        }
                        url
                    }
                    Err(_) if is_production => {
                        tracing::error!(
                            "SECURITY ERROR: BASE_URL must be set in production. \
                             SSO callback URLs require a valid HTTPS base URL. \
                             Set BASE_URL to your HTTPS domain (e.g., https://app.example.com)"
                        );
                        return Err(anyhow::anyhow!(
                            "BASE_URL must be set in production environments"
                        ));
                    }
                    Err(_) => {
                        tracing::warn!(
                            "BASE_URL not set. Using http://localhost:3000 for development. \
                             Set BASE_URL to your HTTPS domain in production."
                        );
                        "http://localhost:3000".to_string()
                    }
                }
            },

            allow_signup: env::var("ALLOW_SIGNUP")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),

            allow_password_login: env::var("ALLOW_PASSWORD_LOGIN")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),

            // Stripe configuration
            stripe_api_key: env::var("STRIPE_API_KEY").ok(),
            stripe_webhook_secret: env::var("STRIPE_WEBHOOK_SECRET").ok(),
            stripe_allowed_price_ids: env::var("STRIPE_ALLOWED_PRICE_IDS")
                .map(|s| {
                    s.split(',')
                        .map(|id| id.trim().to_string())
                        .filter(|id| !id.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            stripe_metered_price_id: env::var("STRIPE_METERED_PRICE_ID").ok(),
            stripe_webhook_ip_allowlist_enabled: env::var("STRIPE_WEBHOOK_IP_ALLOWLIST_ENABLED")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(false),
            stripe_webhook_ip_allowlist: env::var("STRIPE_WEBHOOK_IP_ALLOWLIST")
                .map(|s| {
                    s.split(',')
                        .map(|ip| ip.trim().to_string())
                        .filter(|ip| !ip.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            stripe_portal_return_url: env::var("STRIPE_PORTAL_RETURN_URL")
                .unwrap_or_else(|_| "/settings/billing".to_string()),

            // Billing configuration
            credits_enabled: env::var("CREDITS_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),

            budget_alert_cooldown_hours: env::var("BUDGET_ALERT_COOLDOWN_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(24), // Default: 24 hours

            // AI Gateway configuration
            gateway_fallback_enabled: env::var("GATEWAY_FALLBACK_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(true), // Enabled by default

            gateway_max_retries: env::var("GATEWAY_MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(|v: u32| v.min(5)) // Cap at 5 retries
                .unwrap_or(2),

            gateway_initial_retry_delay_ms: env::var("GATEWAY_INITIAL_RETRY_DELAY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500),

            gateway_max_retry_delay_ms: env::var("GATEWAY_MAX_RETRY_DELAY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),

            gateway_cache_enabled: env::var("GATEWAY_CACHE_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false), // Disabled by default

            gateway_cache_url: env::var("GATEWAY_CACHE_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),

            gateway_cache_ttl_seconds: env::var("GATEWAY_CACHE_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(86_400), // 24 hours

            gateway_log_content: env::var("GATEWAY_LOG_CONTENT")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false), // Disabled by default for privacy

            gateway_timeout_seconds: env::var("GATEWAY_TIMEOUT_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120), // 2 minutes default

            gateway_timeout_openai_seconds: env::var("GATEWAY_TIMEOUT_OPENAI_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),

            gateway_timeout_anthropic_seconds: env::var("GATEWAY_TIMEOUT_ANTHROPIC_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),

            gateway_timeout_google_seconds: env::var("GATEWAY_TIMEOUT_GOOGLE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),

            gateway_timeout_bedrock_seconds: env::var("GATEWAY_TIMEOUT_BEDROCK_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180), // 3 minutes default for Bedrock (cold starts)

            gateway_anthropic_api_version: env::var("GATEWAY_ANTHROPIC_API_VERSION")
                .unwrap_or_else(|_| "2023-06-01".to_string()),

            gateway_openai_base_url: env::var("GATEWAY_OPENAI_BASE_URL").ok(),
            gateway_anthropic_base_url: env::var("GATEWAY_ANTHROPIC_BASE_URL").ok(),
            gateway_google_base_url: env::var("GATEWAY_GOOGLE_BASE_URL").ok(),
            gateway_theta_base_url: env::var("GATEWAY_THETA_BASE_URL").ok(),
            gateway_deepseek_base_url: env::var("GATEWAY_DEEPSEEK_BASE_URL").ok(),
            gateway_xai_base_url: env::var("GATEWAY_XAI_BASE_URL").ok(),
            gateway_mistral_base_url: env::var("GATEWAY_MISTRAL_BASE_URL").ok(),
            gateway_groq_base_url: env::var("GATEWAY_GROQ_BASE_URL").ok(),
            gateway_together_base_url: env::var("GATEWAY_TOGETHER_BASE_URL").ok(),
            gateway_fireworks_base_url: env::var("GATEWAY_FIREWORKS_BASE_URL").ok(),
            gateway_perplexity_base_url: env::var("GATEWAY_PERPLEXITY_BASE_URL").ok(),
            gateway_cohere_base_url: env::var("GATEWAY_COHERE_BASE_URL").ok(),
            gateway_openrouter_base_url: env::var("GATEWAY_OPENROUTER_BASE_URL").ok(),
            gateway_cerebras_base_url: env::var("GATEWAY_CEREBRAS_BASE_URL").ok(),
            gateway_deepinfra_base_url: env::var("GATEWAY_DEEPINFRA_BASE_URL").ok(),
            gateway_alibaba_base_url: env::var("GATEWAY_ALIBABA_BASE_URL").ok(),
            gateway_nvidia_base_url: env::var("GATEWAY_NVIDIA_BASE_URL").ok(),
            gateway_ai21_base_url: env::var("GATEWAY_AI21_BASE_URL").ok(),
            gateway_sambanova_base_url: env::var("GATEWAY_SAMBANOVA_BASE_URL").ok(),
            gateway_lambda_base_url: env::var("GATEWAY_LAMBDA_BASE_URL").ok(),
            gateway_lepton_base_url: env::var("GATEWAY_LEPTON_BASE_URL").ok(),
            gateway_hyperbolic_base_url: env::var("GATEWAY_HYPERBOLIC_BASE_URL").ok(),
            gateway_ovhcloud_base_url: env::var("GATEWAY_OVHCLOUD_BASE_URL").ok(),
            gateway_novita_base_url: env::var("GATEWAY_NOVITA_BASE_URL").ok(),
            gateway_huggingface_base_url: env::var("GATEWAY_HUGGINGFACE_BASE_URL").ok(),
            gateway_cloudflare_base_url: env::var("GATEWAY_CLOUDFLARE_BASE_URL").ok(),
            gateway_azure_openai_base_url: env::var("GATEWAY_AZURE_OPENAI_BASE_URL").ok(),
            gateway_vertex_ai_base_url: env::var("GATEWAY_VERTEX_AI_BASE_URL").ok(),

            gateway_timeout_theta_seconds: env::var("GATEWAY_TIMEOUT_THETA_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),

            gateway_timeout_deepseek_seconds: env::var("GATEWAY_TIMEOUT_DEEPSEEK_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),

            gateway_timeout_openai_compat_seconds: env::var(
                "GATEWAY_TIMEOUT_OPENAI_COMPAT_SECONDS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120),

            gateway_default_openai_api_key: env::var("GATEWAY_DEFAULT_OPENAI_API_KEY").ok(),
            gateway_default_anthropic_api_key: env::var("GATEWAY_DEFAULT_ANTHROPIC_API_KEY").ok(),
            gateway_default_google_api_key: env::var("GATEWAY_DEFAULT_GOOGLE_API_KEY").ok(),
            gateway_default_theta_api_key: env::var("GATEWAY_DEFAULT_THETA_API_KEY").ok(),
            gateway_default_deepseek_api_key: env::var("GATEWAY_DEFAULT_DEEPSEEK_API_KEY").ok(),

            playground_evaluation_model: env::var("PLAYGROUND_EVALUATION_MODEL")
                .unwrap_or_else(|_| "gpt-4o-mini".to_string()),

            // GitHub App integration
            github_app_id: env::var("GITHUB_APP_ID").ok().and_then(|s| s.parse().ok()),
            github_app_name: env::var("GITHUB_APP_NAME").ok(),
            github_app_private_key: env::var("GITHUB_APP_PRIVATE_KEY").ok(),
            github_app_webhook_secret: env::var("GITHUB_APP_WEBHOOK_SECRET").ok(),
            github_webhook_ip_allowlist: env::var("GITHUB_WEBHOOK_IP_ALLOWLIST")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|ip| ip.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            trusted_proxy_cidrs: env::var("TRUSTED_PROXY_CIDRS")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|ip| ip.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            api_base_url: env::var("API_BASE_URL").ok(),

            // Slack App (OAuth + Events API)
            slack_client_id: env::var("SLACK_CLIENT_ID").ok(),
            slack_client_secret: env::var("SLACK_CLIENT_SECRET").ok(),
            slack_signing_secret: env::var("SLACK_SIGNING_SECRET").ok(),

            // Social OAuth login
            oauth_google_client_id: env::var("OAUTH_GOOGLE_CLIENT_ID").ok(),
            oauth_google_client_secret: env::var("OAUTH_GOOGLE_CLIENT_SECRET").ok(),
            oauth_github_client_id: env::var("OAUTH_GITHUB_CLIENT_ID").ok(),
            oauth_github_client_secret: env::var("OAUTH_GITHUB_CLIENT_SECRET").ok(),
            oauth_microsoft_client_id: env::var("OAUTH_MICROSOFT_CLIENT_ID").ok(),
            oauth_microsoft_client_secret: env::var("OAUTH_MICROSOFT_CLIENT_SECRET").ok(),

            // Asset storage configuration
            storage_backend: env::var("STORAGE_BACKEND").unwrap_or_else(|_| "local".to_string()),
            storage_local_path: env::var("STORAGE_LOCAL_PATH")
                .unwrap_or_else(|_| "./data/assets".to_string()),
            storage_local_base_url: env::var("STORAGE_LOCAL_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:3000/api/assets".to_string()),
            storage_s3_bucket: env::var("STORAGE_S3_BUCKET").ok(),
            storage_s3_region: env::var("STORAGE_S3_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
            storage_s3_endpoint: env::var("STORAGE_S3_ENDPOINT").ok(),
            storage_s3_path_style: env::var("STORAGE_S3_PATH_STYLE")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),
            flow_gateway_url: env::var("FLOW_GATEWAY_URL")
                .unwrap_or_else(|_| "http://localhost:3001".to_string()),

            // OpenTelemetry configuration (dogfooding)
            otel_exporter_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .ok()
                .filter(|s| !s.is_empty()),
            otel_project_id: env::var("OTEL_PROJECT_ID").ok().filter(|s| !s.is_empty()),

            // Continuous profiling configuration (CPU + heap)
            profiling_enabled: env::var("PROFILING_ENABLED")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),
            profiling_frequency: env::var("PROFILING_FREQUENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(99),
            profiling_cpu_interval_secs: env::var("PROFILING_CPU_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),
            profiling_heap_interval_secs: env::var("PROFILING_HEAP_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),

            // Loops.so transactional email
            loops_api_key: env::var("LOOPS_API_KEY").ok().filter(|s| !s.is_empty()),
            loops_invite_template_id: env::var("LOOPS_INVITE_TEMPLATE_ID")
                .ok()
                .filter(|s| !s.is_empty()),
            loops_alert_template_id: env::var("LOOPS_ALERT_TEMPLATE_ID")
                .ok()
                .filter(|s| !s.is_empty()),
            loops_welcome_template_id: env::var("LOOPS_WELCOME_TEMPLATE_ID")
                .ok()
                .filter(|s| !s.is_empty()),
            app_url: env::var("APP_URL").ok().filter(|s| !s.is_empty()),
        };

        // Validate security-sensitive configuration
        config.validate_security_settings();

        Ok(config)
    }

    /// Validate security-sensitive configuration settings and log warnings.
    ///
    /// This doesn't return errors for invalid settings (to avoid breaking existing deployments)
    /// but logs warnings so operators can fix them.
    fn validate_security_settings(&self) {
        const MIN_WEBHOOK_SECRET_LENGTH: usize = 32;

        // Validate GitHub webhook secret length
        if let Some(ref secret) = self.github_app_webhook_secret {
            if secret.len() < MIN_WEBHOOK_SECRET_LENGTH {
                tracing::warn!(
                    secret_length = secret.len(),
                    min_recommended = MIN_WEBHOOK_SECRET_LENGTH,
                    "GitHub webhook secret is shorter than recommended. \
                     Consider using a secret with at least {} characters for better security.",
                    MIN_WEBHOOK_SECRET_LENGTH
                );
            }
        }

        // Warn if GitHub App is partially configured
        let has_app_id = self.github_app_id.is_some();
        let has_private_key = self.github_app_private_key.is_some();
        let has_webhook_secret = self.github_app_webhook_secret.is_some();

        if (has_app_id || has_private_key) && !(has_app_id && has_private_key) {
            tracing::warn!(
                "GitHub App is partially configured. Both GITHUB_APP_ID and \
                 GITHUB_APP_PRIVATE_KEY are required for GitHub integration to work."
            );
        }

        if has_app_id && has_private_key && !has_webhook_secret {
            tracing::warn!(
                "GitHub App is configured but GITHUB_APP_WEBHOOK_SECRET is not set. \
                 Webhooks will not be processed without a secret."
            );
        }
    }
}
