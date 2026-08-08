use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{instrument, info, error};

use crate::config::ApiConfig;
use crate::metrics::Metric;

#[derive(Debug, Serialize)]
struct MetricsPayload {
    project_key: String,
    metrics: Vec<Metric>,
}

pub struct HttpClient {
    client: Client,
    api_url: String,
    api_key: String,
}

impl HttpClient {
    pub async fn new(config: &ApiConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .build()
            .context("Failed to create HTTP client")?;
        
        Ok(Self {
            client,
            api_url: config.url.clone(),
            api_key: config.api_key.clone(),
        })
    }
    
    #[instrument(skip(self, metrics), fields(metrics_count = metrics.len(), url = %format!("{}/api/v1/metrics", self.api_url)))]
    pub async fn send_metrics(&self, metrics: &[Metric]) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        
        let payload = MetricsPayload {
            project_key: self.api_key.clone(),
            metrics: metrics.to_vec(),
        };
        
        let url = format!("{}/api/v1/metrics", self.api_url);
        
        let start = std::time::Instant::now();
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to send HTTP request")?;
        
        let duration_ms = start.elapsed().as_millis() as u64;
        info!(duration_ms, "HTTP request completed");
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API returned error status {}: {}", status, body);
        }
        
        Ok(())
    }

    #[instrument(skip(self), fields(url = %format!("{}/api/database-monitoring/explain-plans", self.api_url)))]
    pub async fn send_explain_plan(&self, explain_plan: &ExplainPlanPayload) -> Result<()> {
        let url = format!("{}/api/database-monitoring/explain-plans", self.api_url);
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-reiver-project-key", &self.api_key)
            .json(explain_plan)
            .send()
            .await
            .context("Failed to send explain plan HTTP request")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!("API returned error status {}: {}", status, body);
            anyhow::bail!("API returned error status {}: {}", status, body);
        }
        
        Ok(())
    }

    #[instrument(skip(self), fields(url = %format!("{}/api/database-monitoring/query-metrics", self.api_url)))]
    pub async fn send_query_metrics(&self, query_metrics: &QueryMetricsPayload) -> Result<()> {
        let url = format!("{}/api/database-monitoring/query-metrics", self.api_url);
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-reiver-project-key", &self.api_key)
            .json(query_metrics)
            .send()
            .await
            .context("Failed to send query metrics HTTP request")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!("API returned error status {}: {}", status, body);
            anyhow::bail!("API returned error status {}: {}", status, body);
        }
        
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct ExplainPlanPayload {
    pub project_key: String,
    pub database_name: String,
    pub database_host: String,
    pub database_type: String,
    pub query_template: String,
    pub query_parameters: Option<serde_json::Value>,
    pub explain_plan: serde_json::Value,
    pub execution_time_ms: Option<f64>,
    pub planning_time_ms: Option<f64>,
    pub total_cost: Option<f64>,
    pub rows_estimated: Option<i64>,
    pub rows_actual: Option<i64>,
    pub has_full_table_scan: Option<bool>,
    pub has_missing_index: Option<bool>,
    pub has_sequential_scan: Option<bool>,
    pub trace_id: Option<String>,
    pub query_fingerprint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryMetricsPayload {
    pub project_key: String,
    pub database_name: String,
    pub database_host: String,
    pub database_type: String,
    pub query_fingerprint: String,
    pub query_template: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub min_time_ms: f64,
    pub max_time_ms: f64,
    pub stddev_time_ms: Option<f64>,
    pub rows_affected: Option<i64>,
    pub rows_returned: Option<i64>,
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

