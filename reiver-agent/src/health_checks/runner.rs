//! Health Check Runner
//!
//! Manages scheduling and execution of health checks, and reports results.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::{HealthCheckConfig, HealthCheckResult, HttpHealthCheck, TcpHealthCheck, SslCertificateCheck};

/// Response from the maintenance windows active endpoint
#[derive(Debug, serde::Deserialize)]
struct MaintenanceWindowResponse {
    project_id: String,
}

/// Runs health checks on a schedule
pub struct HealthCheckRunner {
    /// Agent ID for tracking which agent ran the check
    agent_id: String,
    /// Location identifier
    location: String,
    /// Endpoint to report results to
    report_endpoint: String,
    /// Base API endpoint for maintenance window sync
    api_base: String,
    /// HTTP client for reporting
    client: reqwest::Client,
    /// Active health checks
    checks: Arc<RwLock<HashMap<String, HealthCheckConfig>>>,
    /// Projects currently in maintenance (synced periodically)
    projects_in_maintenance: Arc<RwLock<HashSet<String>>>,
}

impl HealthCheckRunner {
    pub fn new(agent_id: String, location: String, report_endpoint: String, api_base: String) -> Self {
        Self {
            agent_id,
            location,
            report_endpoint,
            api_base,
            client: reqwest::Client::new(),
            checks: Arc::new(RwLock::new(HashMap::new())),
            projects_in_maintenance: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Add or update a health check
    pub async fn add_check(&self, config: HealthCheckConfig) {
        let mut checks = self.checks.write().await;
        info!("Adding health check: {} ({})", config.name, config.check_type);
        checks.insert(config.id.clone(), config);
    }

    /// Remove a health check
    pub async fn remove_check(&self, check_id: &str) {
        let mut checks = self.checks.write().await;
        if checks.remove(check_id).is_some() {
            info!("Removed health check: {}", check_id);
        }
    }

    /// Start the health check runner
    pub async fn start(&self) {
        info!("Starting health check runner");

        // Run checks every second (individual checks have their own intervals)
        let mut tick_interval = interval(Duration::from_secs(1));
        let mut last_run: HashMap<String, i64> = HashMap::new();
        let mut last_maintenance_sync = Instant::now() - Duration::from_secs(60); // Force initial sync

        loop {
            tick_interval.tick().await;

            let now = chrono::Utc::now().timestamp();

            // Sync maintenance windows every 30 seconds
            if last_maintenance_sync.elapsed() >= Duration::from_secs(30) {
                self.sync_maintenance_windows().await;
                last_maintenance_sync = Instant::now();
            }

            // Get current projects in maintenance
            let in_maintenance = self.projects_in_maintenance.read().await.clone();

            // Collect checks that need to run
            let checks_to_run: Vec<HealthCheckConfig> = {
                let checks = self.checks.read().await;
                checks.iter()
                    .filter(|(id, config)| {
                        let last = last_run.get(*id).copied().unwrap_or(0);
                        now - last >= config.check_interval_seconds as i64
                    })
                    .filter(|(_, config)| {
                        // Skip checks for projects in maintenance
                        if in_maintenance.contains(&config.project_id) {
                            debug!("Skipping health check {} - project {} is in maintenance",
                                   config.name, config.project_id);
                            false
                        } else {
                            true
                        }
                    })
                    .map(|(_, config)| config.clone())
                    .collect()
            };

            for config in checks_to_run {
                // Update last run time
                last_run.insert(config.id.clone(), now);

                // Run the check
                let agent_id = self.agent_id.clone();
                let location = self.location.clone();
                let client = self.client.clone();
                let report_endpoint = self.report_endpoint.clone();

                tokio::spawn(async move {
                    let result = Self::run_check(&config).await;

                    // Add agent info
                    let result_with_agent = HealthCheckResultWithAgent {
                        result,
                        agent_id,
                        agent_location: location,
                    };

                    // Report result
                    if let Err(e) = Self::report_result(&client, &report_endpoint, &result_with_agent).await {
                        error!("Failed to report health check result: {}", e);
                    }
                });
            }
        }
    }

    /// Sync active maintenance windows from the server
    async fn sync_maintenance_windows(&self) {
        if self.api_base.is_empty() {
            return;
        }

        // Get all unique project IDs from checks
        let project_ids: Vec<String> = {
            let checks = self.checks.read().await;
            let mut ids: HashSet<_> = checks.values().map(|c| c.project_id.clone()).collect();
            ids.into_iter().collect()
        };

        if project_ids.is_empty() {
            return;
        }

        let mut new_maintenance_set = HashSet::new();

        // Query maintenance windows for each project
        for project_id in &project_ids {
            let url = format!("{}/maintenance-windows/active?project_id={}", self.api_base, project_id);
            match self.client
                .get(&url)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<Vec<MaintenanceWindowResponse>>().await {
                            Ok(windows) => {
                                if !windows.is_empty() {
                                    new_maintenance_set.insert(project_id.clone());
                                    debug!("Project {} is in maintenance ({} active windows)",
                                           project_id, windows.len());
                                }
                            }
                            Err(e) => {
                                debug!("Failed to parse maintenance windows response: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Failed to fetch maintenance windows for project {}: {}", project_id, e);
                }
            }
        }

        // Update the maintenance set
        let mut maintenance = self.projects_in_maintenance.write().await;
        *maintenance = new_maintenance_set;
    }

    async fn run_check(config: &HealthCheckConfig) -> HealthCheckResult {
        match config.check_type.as_str() {
            "http" | "https" => HttpHealthCheck::check(config).await,
            "tcp" => TcpHealthCheck::check(config).await,
            "udp" => TcpHealthCheck::check_udp(config).await,
            "ssl" | "tls" => SslCertificateCheck::check(config).await,
            _ => {
                error!("Unknown check type: {}", config.check_type);
                HealthCheckResult {
                    check_id: config.id.clone(),
                    check_type: config.check_type.clone(),
                    check_name: config.name.clone(),
                    target: "".to_string(),
                    status: super::CheckStatus::Error,
                    success: false,
                    response_time_ms: 0.0,
                    dns_time_ms: None,
                    connect_time_ms: None,
                    tls_time_ms: None,
                    first_byte_time_ms: None,
                    http_status_code: None,
                    http_response_size: None,
                    ssl_valid: false,
                    ssl_days_until_expiry: None,
                    ssl_issuer: None,
                    ssl_subject: None,
                    ssl_expires_at: None,
                    error_type: Some("configuration".to_string()),
                    error_message: Some(format!("Unknown check type: {}", config.check_type)),
                    response_body: None,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                }
            }
        }
    }

    async fn report_result(
        client: &reqwest::Client,
        endpoint: &str,
        result: &HealthCheckResultWithAgent,
    ) -> Result<()> {
        if endpoint.is_empty() {
            debug!("No report endpoint configured, skipping result report");
            return Ok(());
        }

        let response = client
            .post(endpoint)
            .json(result)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("Failed to report health check result: {} - {}", status, body);
        }

        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
struct HealthCheckResultWithAgent {
    #[serde(flatten)]
    result: HealthCheckResult,
    agent_id: String,
    agent_location: String,
}
