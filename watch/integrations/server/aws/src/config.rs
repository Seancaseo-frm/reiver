//! Configuration for AWS integrations

use serde::{Deserialize, Serialize};
use aws_config::{SdkConfig, Region};
use aws_sdk_sts::Client as StsClient;
use tracing::info;

/// AWS integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    
    // IAM Role Delegation (preferred method, like Datadog)
    /// IAM role ARN to assume (preferred method)
    pub role_arn: Option<String>,
    /// External ID for role assumption (prevents confused deputy attacks)
    pub external_id: Option<String>,
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            role_arn: None,
            external_id: None,
        }
    }
}

impl AwsConfig {
    /// Create AWS SDK config from this configuration
    /// 
    /// Priority:
    /// 1. IAM Role Delegation (if role_arn is provided) - preferred method
    /// 2. Default credential chain (environment variables, ~/.aws/credentials, IAM roles)
    /// 
    /// For IAM role delegation (like Datadog):
    /// - Uses STS to assume the role
    /// - Generates temporary credentials
    /// - More secure than storing static access keys
    pub async fn into_aws_config(&self) -> Result<SdkConfig, anyhow::Error> {
        let region = Region::new(self.region.clone());
        
        // Priority 1: IAM Role Delegation (preferred, like Datadog)
        if let Some(role_arn) = &self.role_arn {
            info!("Using IAM role delegation: {}", role_arn);
            return self.assume_role(role_arn, self.external_id.as_deref(), region).await;
        }
        
        // Priority 2: Default credential chain
        info!("Using default AWS credential chain");
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        loader = loader.region(region);
        Ok(loader.load().await)
    }
    
    /// Assume an IAM role using STS (Security Token Service)
    /// This is the secure method used by Datadog and other AWS integrations
    async fn assume_role(
        &self,
        role_arn: &str,
        external_id: Option<&str>,
        region: Region,
    ) -> Result<SdkConfig, anyhow::Error> {
        // First, create a base config to get STS client
        // This uses the default credential chain (could be instance role, env vars, etc.)
        let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region.clone())
            .load()
            .await;
        
        let sts_client = StsClient::new(&base_config);
        
        // Build assume role request
        let mut assume_role_request = sts_client
            .assume_role()
            .role_arn(role_arn)
            .role_session_name("reiver-integration"); // Session name for audit trail
        
        // Add external ID if provided (security best practice)
        if let Some(external_id) = external_id {
            assume_role_request = assume_role_request.external_id(external_id);
        }
        
        // Assume the role
        let response = assume_role_request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to assume IAM role {}: {}", role_arn, e))?;
        
        // Extract temporary credentials from response
        let credentials = response
            .credentials()
            .ok_or_else(|| anyhow::anyhow!("No credentials in assume role response"))?;
        
        // Extract credentials - all return &str
        let access_key_id = credentials.access_key_id();
        let secret_access_key = credentials.secret_access_key();
        // session_token() may return Option<&str> or &str depending on SDK version
        // Handle both cases
        let session_token = credentials.session_token();
        
        info!("Successfully assumed IAM role: {}", role_arn);
        
        // Create new config with temporary credentials
        // Use environment variables approach (simpler and works with default credential chain)
        std::env::set_var("AWS_ACCESS_KEY_ID", access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_access_key);
        // session_token is always present for STS assume role responses
        std::env::set_var("AWS_SESSION_TOKEN", session_token);
        
        // Create config using environment variables (which we just set)
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        loader = loader.region(region);
        
        Ok(loader.load().await)
    }
}

