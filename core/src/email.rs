//! Loops.so transactional email client.

use serde::Serialize;
use tracing::info;

const LOOPS_API_URL: &str = "https://app.loops.so/api/v1/transactional";

pub struct LoopsClient {
    http: reqwest::Client,
    api_key: String,
    invite_template_id: String,
    alert_template_id: String,
    welcome_template_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InviteVars {
    pub inviter_name: String,
    pub organization_name: String,
    pub invite_url: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertVars {
    pub alert_name: String,
    pub severity: String,
    pub message: String,
    pub project_name: String,
    pub dashboard_url: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WelcomeVars {
    pub first_name: String,
}

impl LoopsClient {
    pub fn new(
        api_key: String,
        invite_template_id: String,
        alert_template_id: String,
        welcome_template_id: String,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            api_key,
            invite_template_id,
            alert_template_id,
            welcome_template_id,
        }
    }

    pub async fn send_invite(&self, to: &str, vars: InviteVars) -> Result<(), anyhow::Error> {
        let idempotency_key = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "transactionalId": self.invite_template_id,
            "email": to,
            "dataVariables": vars,
            "addToAudience": false,
        });

        let resp = self
            .http
            .post(LOOPS_API_URL)
            .bearer_auth(&self.api_key)
            .header("X-Idempotency-Key", &idempotency_key)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Loops invite email failed: {} - {}",
                status,
                resp_body
            ));
        }

        info!(to = to, "Sent invite email via Loops");
        Ok(())
    }

    pub async fn send_alert(&self, to: &str, vars: AlertVars) -> Result<(), anyhow::Error> {
        let idempotency_key = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "transactionalId": self.alert_template_id,
            "email": to,
            "dataVariables": vars,
            "addToAudience": false,
        });

        let resp = self
            .http
            .post(LOOPS_API_URL)
            .bearer_auth(&self.api_key)
            .header("X-Idempotency-Key", &idempotency_key)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Loops alert email failed: {} - {}",
                status,
                resp_body
            ));
        }

        info!(to = to, "Sent alert email via Loops");
        Ok(())
    }

    pub async fn send_welcome(&self, to: &str, vars: WelcomeVars) -> Result<(), anyhow::Error> {
        let idempotency_key = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "transactionalId": self.welcome_template_id,
            "email": to,
            "dataVariables": vars,
            "addToAudience": true,
        });

        let resp = self
            .http
            .post(LOOPS_API_URL)
            .bearer_auth(&self.api_key)
            .header("X-Idempotency-Key", &idempotency_key)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Loops welcome email failed: {} - {}",
                status,
                resp_body
            ));
        }

        info!(to = to, "Sent welcome email via Loops");
        Ok(())
    }
}
