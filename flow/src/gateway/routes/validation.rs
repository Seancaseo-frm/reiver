//! Request validation, authentication, PII masking, and session budget management.

use bb8_redis::redis::AsyncCommands;
use std::time::Duration;
use uuid::Uuid;

use crate::app_state::FlowState;
use crate::gateway::error::GatewayError;
use crate::gateway::types::ChatCompletionRequest;

/// Mask PII in request messages before they reach any external provider.
///
/// Returns `true` if any PII was detected and masked. Delegates to the
/// shared `mask_pii_text` primitive for the actual redaction.
pub(super) async fn mask_request_pii(
    state: &FlowState,
    project_id: Uuid,
    request: &mut ChatCompletionRequest,
) -> bool {
    use std::borrow::Cow;

    use crate::gateway::guardrails::mask_pii_text;
    use crate::gateway::types::{ContentPart, MessageContent};

    let enabled = reiver_core::pii::get_pii_masking_enabled_cached(
        state.redis.as_ref(),
        state.db.as_ref(),
        project_id,
    )
    .await;
    if !enabled {
        return false;
    }

    let mut detected = false;
    for msg in &mut request.messages {
        if let Some(ref mut content) = msg.content {
            match content {
                MessageContent::Text(s) => {
                    if let Cow::Owned(masked) = mask_pii_text(s) {
                        detected = true;
                        *s = masked;
                    }
                }
                MessageContent::Parts(parts) => {
                    for part in parts.iter_mut() {
                        if let ContentPart::Text { text } = part {
                            if let Cow::Owned(masked) = mask_pii_text(text) {
                                detected = true;
                                *text = masked;
                            }
                        }
                    }
                }
            }
        }
    }
    detected
}

/// Pre-request session cost budget check.
///
/// Reads accumulated spend from Redis and rejects the request if the budget
/// has already been reached. Fails open if Redis is unavailable.
#[tracing::instrument(
    name = "gateway.session_budget.check",
    skip(state),
    fields(project_id = %project_id)
)]
pub(super) async fn check_session_budget_pre(
    state: &FlowState,
    project_id: Uuid,
    session_id: &str,
    budget: Option<f64>,
    request_id: &str,
) -> Result<(), GatewayError> {
    let budget = match budget {
        Some(b) if !session_id.is_empty() => b,
        _ => return Ok(()),
    };

    let redis_key = format!("session:budget:{}:{}", project_id, session_id);
    let accumulated: f64 = if let Ok(mut conn) = state.redis.get().await {
        let val: Option<String> =
            tokio::time::timeout(Duration::from_secs(1), conn.get(&redis_key))
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten();
        val.and_then(|v| v.parse().ok()).unwrap_or(0.0)
    } else {
        0.0
    };

    if accumulated >= budget {
        tracing::info!(
            request_id = %request_id,
            project_id = %project_id,
            session_id = %session_id,
            accumulated_usd = %accumulated,
            budget_usd = %budget,
            "Session cost budget exceeded, rejecting request"
        );
        return Err(GatewayError::SessionBudgetExceeded {
            limit_usd: budget,
            used_usd: accumulated,
            session_id: session_id.to_string(),
        });
    }
    Ok(())
}

/// Maximum session cost (USD) we record as f64; larger values are clamped and logged.
pub(super) const MAX_SESSION_COST_F64: f64 = 1e15;

/// Convert a cost `Decimal` to f64 for Redis session budget. Clamps to [0, MAX_SESSION_COST_F64]
/// and logs a warning on conversion failure or when clamping, so operators see anomalies.
pub(super) fn session_cost_to_f64(cost: rust_decimal::Decimal) -> f64 {
    let raw = match f64::try_from(cost) {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(
                cost = %cost,
                "Session budget cost conversion failed (e.g. overflow); using 0 to avoid panic"
            );
            return 0.0;
        }
    };
    if !raw.is_finite() || raw < 0.0 {
        tracing::warn!(
            cost = %cost,
            raw = %raw,
            "Session budget cost non-finite or negative; using 0"
        );
        return 0.0;
    }
    if raw > MAX_SESSION_COST_F64 {
        tracing::warn!(
            cost = %cost,
            clamped = MAX_SESSION_COST_F64,
            "Session budget cost exceeded representable cap; clamped for Redis"
        );
        return MAX_SESSION_COST_F64;
    }
    raw
}

/// Atomically increment the per-session cost accumulator in Redis,
/// guarded by the budget limit.
///
/// Uses a Lua script so the read-check-increment is a single atomic
/// operation, preventing two concurrent requests from both passing
/// the budget check and overspending.
///
/// Fails silently if Redis is unavailable.
#[tracing::instrument(
    name = "gateway.session_budget.increment",
    skip(state, usage, budget),
    fields(project_id = %project_id)
)]
pub(super) async fn increment_session_budget(
    state: &FlowState,
    project_id: Uuid,
    session_id: &str,
    provider: &str,
    model: &str,
    usage: &crate::gateway::types::Usage,
    budget: Option<f64>,
) {
    if let Ok(cost) = state
        .llm_processor
        .cost_calculator()
        .calculate_cost(
            provider,
            model,
            usage.prompt_tokens,
            usage.completion_tokens,
            0,
            0,
        )
        .await
    {
        if cost > rust_decimal::Decimal::ZERO {
            let cost_f64 = session_cost_to_f64(cost);
            let redis_key = format!("session:budget:{}:{}", project_id, session_id);
            if let Ok(mut conn) = state.redis.get().await {
                let _ = tokio::time::timeout(Duration::from_secs(1), async {
                    if let Some(limit) = budget {
                        let script = bb8_redis::redis::Script::new(BUDGET_CHECK_AND_INCREMENT_LUA);
                        let _: Result<i32, _> = script
                            .key(&redis_key)
                            .arg(limit)
                            .arg(cost_f64)
                            .invoke_async(&mut *conn)
                            .await;
                    } else {
                        let _: Result<f64, _> = bb8_redis::redis::cmd("INCRBYFLOAT")
                            .arg(&redis_key)
                            .arg(cost_f64)
                            .query_async(&mut *conn)
                            .await;
                        let _: Result<bool, _> = conn.expire(&redis_key, 86400).await;
                    }
                })
                .await;
            }
        }
    }
}

/// Lua script for atomic budget check + increment.
///
/// Always increments the cost counter and sets the TTL, even when the budget
/// is exceeded. This is critical because the LLM call has already completed
/// by the time this script runs -- refusing to record the cost would make
/// the tracker under-report actual spend, causing `check_session_budget_pre`
/// to see stale values and allow infinite overspending.
///
/// Returns 1 if the new total is within the budget, 0 if it exceeds it.
pub(super) const BUDGET_CHECK_AND_INCREMENT_LUA: &str = r#"
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local new_total = current + tonumber(ARGV[2])
redis.call('INCRBYFLOAT', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], 86400)
if new_total > tonumber(ARGV[1]) then return 0 end
return 1
"#;

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the budget Lua script must reference KEYS[1], ARGV[1], ARGV[2]
    /// and include GET, INCRBYFLOAT, and EXPIRE to be atomic. This test ensures
    /// the script constant is syntactically consistent with those expectations.
    #[test]
    fn test_budget_lua_script_structure() {
        let script = BUDGET_CHECK_AND_INCREMENT_LUA;
        assert!(
            script.contains("KEYS[1]"),
            "script must use KEYS[1] for the redis key"
        );
        assert!(
            script.contains("ARGV[1]"),
            "script must use ARGV[1] for the budget limit"
        );
        assert!(
            script.contains("ARGV[2]"),
            "script must use ARGV[2] for the cost increment"
        );
        assert!(
            script.contains("INCRBYFLOAT"),
            "script must atomically increment"
        );
        assert!(script.contains("EXPIRE"), "script must set TTL on the key");
        assert!(
            script.contains("return 0") && script.contains("return 1"),
            "script must return 0 (rejected) or 1 (accepted)"
        );
    }

    /// Regression: the Lua script must ALWAYS call INCRBYFLOAT before any
    /// `return 0` that indicates budget exceeded.  Previously the script
    /// returned 0 without incrementing, causing the cost tracker to
    /// under-report actual spend and breaking budget enforcement.
    #[test]
    fn test_budget_lua_script_always_increments_before_returning() {
        let script = BUDGET_CHECK_AND_INCREMENT_LUA;
        let incr_pos = script
            .find("INCRBYFLOAT")
            .expect("script must contain INCRBYFLOAT");
        let expire_pos = script.find("EXPIRE").expect("script must contain EXPIRE");

        for (idx, _) in script.match_indices("return 0") {
            assert!(
                incr_pos < idx,
                "INCRBYFLOAT (pos {}) must appear before `return 0` (pos {}); \
                 otherwise the cost is not tracked when the budget is exceeded",
                incr_pos,
                idx
            );
            assert!(
                expire_pos < idx,
                "EXPIRE (pos {}) must appear before `return 0` (pos {}); \
                 otherwise the TTL is lost when the budget is exceeded",
                expire_pos,
                idx
            );
        }
        for (idx, _) in script.match_indices("return 1") {
            assert!(
                incr_pos < idx,
                "INCRBYFLOAT (pos {}) must appear before `return 1` (pos {})",
                incr_pos,
                idx
            );
        }
    }

    /// Regression: session_cost_to_f64 must never panic; conversion failure or invalid
    /// values must yield a defined result (0 or clamped).
    #[test]
    fn test_session_cost_to_f64_never_panics() {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        // Normal positive value
        let v = session_cost_to_f64(Decimal::from(1));
        assert!(
            v >= 0.0 && v.is_finite(),
            "normal cost must be finite and non-negative"
        );
        assert!((v - 1.0).abs() < 1e-10);

        // Negative must return 0 without panicking
        let v = session_cost_to_f64(Decimal::from(-1));
        assert_eq!(v, 0.0);

        // Zero
        assert_eq!(session_cost_to_f64(Decimal::ZERO), 0.0);

        // Large value must be clamped to MAX_SESSION_COST_F64 without panicking
        let large = Decimal::from_str("1000000000000001").unwrap(); // > 1e15
        let v = session_cost_to_f64(large);
        assert!(v.is_finite() && v >= 0.0);
        assert_eq!(v, MAX_SESSION_COST_F64, "value over cap must be clamped");
    }
}
