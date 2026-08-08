use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use std::time::Duration;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize)]
pub struct VerificationPayload {
    pub requester_email: String,
    pub requester_org_name: String,
    pub requester_org_id: Uuid,
    pub target_agent_id: Uuid,
    pub target_agent_name: String,
}

/// Compute HMAC-SHA256 signature for a payload using the given secret.
pub fn sign_payload(secret: &str, body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Call the target org's verification webhook with HMAC-SHA256 signing.
///
/// Returns `Ok(true)` for approved (HTTP 200), `Ok(false)` for denied
/// (any other status), and `Err` on transport failures.
pub async fn call_verification_webhook(
    http_client: &reqwest::Client,
    verification_url: &str,
    webhook_secret: &str,
    payload: &VerificationPayload,
) -> Result<bool, String> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| format!("Failed to serialize verification payload: {e}"))?;

    let signature = sign_payload(webhook_secret, &body);

    let resp = http_client
        .post(verification_url)
        .header("Content-Type", "application/json")
        .header("X-Herd-Signature", signature)
        .body(body)
        .timeout(WEBHOOK_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("Verification webhook request failed: {e}"))?;

    let status = resp.status();
    tracing::info!(
        webhook.url = verification_url,
        webhook.status = %status,
        requester.org_id = %payload.requester_org_id,
        "Verification webhook responded"
    );

    Ok(status.is_success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_serializes_to_json() {
        let payload = VerificationPayload {
            requester_email: "alice@example.com".into(),
            requester_org_name: "Acme Corp".into(),
            requester_org_id: Uuid::nil(),
            target_agent_id: Uuid::nil(),
            target_agent_name: "Support Bot".into(),
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["requester_email"], "alice@example.com");
        assert_eq!(json["requester_org_name"], "Acme Corp");
        assert_eq!(json["target_agent_name"], "Support Bot");
    }

    #[test]
    fn hmac_signature_is_deterministic() {
        let secret = "test-secret-key";
        let body = b"{\"requester_email\":\"a@b.com\"}";

        let mut mac1 = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac1.update(body);
        let sig1 = hex::encode(mac1.finalize().into_bytes());

        let mut mac2 = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac2.update(body);
        let sig2 = hex::encode(mac2.finalize().into_bytes());

        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 64); // SHA-256 produces 32 bytes = 64 hex chars
    }
}
