use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use stripe::Client;
use stripe_billing::billing_meter_event::CreateBillingMeterEvent;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::db::DbPool;

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const CHANNEL_CAPACITY: usize = 10_000;
const MAX_RETRIES_PER_EVENT: u8 = 3;
const CACHE_PURGE_INTERVAL: u32 = 360;

/// Which Stripe Billing Meter an event targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeterName {
    MoodengCredits,
    SessionScans,
    ObservabilityGb,
}

impl MeterName {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::MoodengCredits => "moodeng_credits",
            Self::SessionScans => "session_scans",
            Self::ObservabilityGb => "observability_gb",
        }
    }
}

/// A single metering event in the internal channel.
struct UsageEvent {
    meter: MeterName,
    organization_id: Uuid,
    value: i64,
    idempotency_key: String,
}

/// Multi-meter service that batches usage events and flushes them to Stripe Billing Meters.
///
/// Callers use typed methods (`record_credits`, `record_scan`, `record_observability_gb`)
/// which are non-blocking (send to an internal mpsc channel). A background task flushes
/// events to Stripe every 5 seconds with per-event idempotency keys.
#[derive(Clone)]
pub struct MeterService {
    tx: mpsc::Sender<UsageEvent>,
    enabled: Arc<AtomicBool>,
}

impl MeterService {
    pub fn new(stripe_client: Client, db: Arc<DbPool>) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(flush_loop(rx, stripe_client, db));
        Self {
            tx,
            enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn from_api_key(api_key: &str, db: Arc<DbPool>) -> Self {
        Self::new(Client::new(api_key), db)
    }

    /// Create a no-op meter service that discards all events silently.
    pub fn noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self {
            tx,
            enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Record credit usage from an MCP action.
    /// `idempotency_key` should be the action execution ID.
    pub fn record_credits(&self, organization_id: Uuid, credits: i64, idempotency_key: String) {
        self.send_event(MeterName::MoodengCredits, organization_id, credits, idempotency_key);
    }

    /// Record a single session scan.
    /// `idempotency_key` should be the scan/evaluation job ID.
    pub fn record_scan(&self, organization_id: Uuid, idempotency_key: String) {
        self.send_event(MeterName::SessionScans, organization_id, 1, idempotency_key);
    }

    /// Record observability ingestion in whole GB.
    /// `idempotency_key` should be `{org_id}-{hour_timestamp}` or a batch ID.
    pub fn record_observability_gb(&self, organization_id: Uuid, gb: i64, idempotency_key: String) {
        self.send_event(MeterName::ObservabilityGb, organization_id, gb, idempotency_key);
    }

    /// Backward-compat: record raw USD usage on the legacy platform_usage_usd meter.
    /// Deprecated — will be removed once all callers migrate to typed methods.
    #[deprecated(note = "Use record_credits/record_scan/record_observability_gb instead")]
    pub fn record_usage(&self, organization_id: Uuid, cost_usd: Decimal) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let cost_cents = decimal_to_cents(cost_usd);
        if cost_cents <= 0 {
            return;
        }
        let key = format!("{}-{}", organization_id, chrono::Utc::now().timestamp_millis());
        if let Err(e) = self.tx.try_send(UsageEvent {
            meter: MeterName::MoodengCredits,
            organization_id,
            value: cost_cents,
            idempotency_key: key,
        }) {
            warn!(
                organization_id = %organization_id,
                error = %e,
                "MeterService channel full or closed, dropping legacy usage event"
            );
        }
    }

    fn send_event(&self, meter: MeterName, organization_id: Uuid, value: i64, idempotency_key: String) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if value <= 0 {
            return;
        }
        if let Err(e) = self.tx.try_send(UsageEvent {
            meter,
            organization_id,
            value,
            idempotency_key,
        }) {
            warn!(
                organization_id = %organization_id,
                meter = meter.event_name(),
                value = value,
                error = %e,
                "MeterService channel full or closed, dropping meter event"
            );
        }
    }
}

fn decimal_to_cents(usd: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    (usd * Decimal::from(100))
        .round_dp(0)
        .to_i64()
        .unwrap_or(0)
}

/// Pending event ready to flush to Stripe (already resolved to a customer ID).
struct PendingFlush {
    meter: MeterName,
    organization_id: Uuid,
    value: i64,
    idempotency_key: String,
    retries: u8,
}

/// Background loop: drain channel, resolve customer IDs, flush to Stripe per-event.
async fn flush_loop(mut rx: mpsc::Receiver<UsageEvent>, client: Client, db: Arc<DbPool>) {
    let mut customer_cache: HashMap<Uuid, String> = HashMap::new();
    let mut pending: Vec<PendingFlush> = Vec::new();
    let mut flush_count: u32 = 0;

    loop {
        let first = match tokio::time::timeout(FLUSH_INTERVAL, rx.recv()).await {
            Ok(Some(event)) => Some(event),
            Ok(None) => {
                debug!("MeterService channel closed, flushing remaining events");
                flush_pending(&mut pending, &customer_cache, &client, &db).await;
                return;
            }
            Err(_) => None,
        };

        if let Some(event) = first {
            pending.push(PendingFlush {
                meter: event.meter,
                organization_id: event.organization_id,
                value: event.value,
                idempotency_key: event.idempotency_key,
                retries: 0,
            });
        }

        while let Ok(event) = rx.try_recv() {
            pending.push(PendingFlush {
                meter: event.meter,
                organization_id: event.organization_id,
                value: event.value,
                idempotency_key: event.idempotency_key,
                retries: 0,
            });
        }

        if pending.is_empty() {
            continue;
        }

        for p in &pending {
            if !customer_cache.contains_key(&p.organization_id) {
                if let Some(cust_id) = resolve_stripe_customer_id(&db, p.organization_id).await {
                    customer_cache.insert(p.organization_id, cust_id);
                }
            }
        }

        flush_pending(&mut pending, &customer_cache, &client, &db).await;

        flush_count += 1;
        if flush_count >= CACHE_PURGE_INTERVAL {
            flush_count = 0;
            customer_cache.clear();
            debug!("Purged customer ID cache");
        }
    }
}

async fn flush_pending(
    pending: &mut Vec<PendingFlush>,
    customer_cache: &HashMap<Uuid, String>,
    client: &Client,
    _db: &DbPool,
) {
    let mut retry_queue: Vec<PendingFlush> = Vec::new();

    for event in pending.drain(..) {
        let stripe_customer_id = match customer_cache.get(&event.organization_id) {
            Some(id) => id,
            None => {
                warn!(
                    organization_id = %event.organization_id,
                    meter = event.meter.event_name(),
                    value = event.value,
                    "No Stripe customer ID for org, dropping meter event"
                );
                continue;
            }
        };

        let payload: HashMap<String, String> = [
            ("stripe_customer_id".to_string(), stripe_customer_id.clone()),
            ("value".to_string(), event.value.to_string()),
        ]
        .into_iter()
        .collect();

        trace!(
            organization_id = %event.organization_id,
            meter = event.meter.event_name(),
            value = event.value,
            idempotency_key = %event.idempotency_key,
            "Flushing meter event to Stripe"
        );

        match CreateBillingMeterEvent::new(event.meter.event_name(), payload)
            .identifier(&event.idempotency_key)
            .send(client)
            .await
        {
            Ok(_) => {
                trace!(
                    organization_id = %event.organization_id,
                    meter = event.meter.event_name(),
                    value = event.value,
                    "Meter event sent"
                );
            }
            Err(e) => {
                if event.retries >= MAX_RETRIES_PER_EVENT {
                    error!(
                        organization_id = %event.organization_id,
                        meter = event.meter.event_name(),
                        value = event.value,
                        retries = event.retries,
                        error = %e,
                        "Dropping meter event after max retries"
                    );
                } else {
                    error!(
                        organization_id = %event.organization_id,
                        meter = event.meter.event_name(),
                        error = %e,
                        "Failed to send meter event, will retry"
                    );
                    retry_queue.push(PendingFlush {
                        retries: event.retries + 1,
                        ..event
                    });
                }
            }
        }
    }

    *pending = retry_queue;
}

async fn resolve_stripe_customer_id(db: &DbPool, organization_id: Uuid) -> Option<String> {
    match sqlx::query_scalar::<_, String>(
        "SELECT stripe_customer_id FROM stripe_customers WHERE organization_id = $1 LIMIT 1",
    )
    .bind(organization_id)
    .fetch_optional(db)
    .await
    {
        Ok(Some(id)) => Some(id),
        Ok(None) => {
            warn!(
                organization_id = %organization_id,
                "No Stripe customer found for organization"
            );
            None
        }
        Err(e) => {
            error!(
                organization_id = %organization_id,
                error = %e,
                "Failed to look up Stripe customer"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_decimal_to_cents() {
        assert_eq!(decimal_to_cents(dec!(1.00)), 100);
        assert_eq!(decimal_to_cents(dec!(0.01)), 1);
        assert_eq!(decimal_to_cents(dec!(0.006)), 1);
        assert_eq!(decimal_to_cents(dec!(0.004)), 0);
        assert_eq!(decimal_to_cents(dec!(0.00)), 0);
        assert_eq!(decimal_to_cents(dec!(-1.00)), -100);
    }

    #[test]
    fn test_noop_does_not_log_or_panic() {
        let svc = MeterService::noop();
        assert!(!svc.enabled.load(Ordering::Relaxed));
        svc.record_credits(Uuid::new_v4(), 5, "test-key".to_string());
        svc.record_scan(Uuid::new_v4(), "scan-key".to_string());
        svc.record_observability_gb(Uuid::new_v4(), 10, "gb-key".to_string());
    }

    #[test]
    fn test_zero_value_not_sent() {
        let svc = MeterService::noop();
        svc.record_credits(Uuid::new_v4(), 0, "zero".to_string());
        svc.record_credits(Uuid::new_v4(), -1, "neg".to_string());
        svc.record_observability_gb(Uuid::new_v4(), 0, "zero-gb".to_string());
    }

    #[test]
    fn test_meter_event_names() {
        assert_eq!(MeterName::MoodengCredits.event_name(), "moodeng_credits");
        assert_eq!(MeterName::SessionScans.event_name(), "session_scans");
        assert_eq!(MeterName::ObservabilityGb.event_name(), "observability_gb");
    }
}
