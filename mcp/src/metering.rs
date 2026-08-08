use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::action::ActionContext;

/// Credit weights per action type.
///
/// The spec defines weights by the inner action discriminator, but our architecture
/// uses 5 facade tools. The mapping:
/// - search/get/list → query, classify, search (weight 1)
/// - analyze → summarise_trace (weight 2)
/// - execute → varies by resource/action, but averaged across investigate(3),
///   recommend(3), report(5), compiler(10). We use the action's resource/action
///   pair to determine the correct weight.
const FACADE_WEIGHTS: &[(&str, i64)] = &[
    ("search", 1),
    ("get", 1),
    ("list", 1),
    ("analyze", 2),
    ("execute", 3), // base weight for execute, overridden by action_weight()
];

/// Fine-grained weights for execute actions, keyed by resource/action pair.
/// Falls back to FACADE_WEIGHTS if no match.
const EXECUTE_WEIGHTS: &[(&str, i64)] = &[
    // report-class actions (weight 5)
    ("report", 5),
    ("export", 5),
    ("generate", 5),
    // compiler (weight 10)
    ("compiler", 10),
    ("compile", 10),
];

/// Look up the credit weight for a tool call.
/// For "execute" tools, checks the resource/action for finer granularity.
pub fn credit_weight(tool_name: &str) -> i64 {
    FACADE_WEIGHTS
        .iter()
        .find(|(name, _)| *name == tool_name)
        .map(|(_, w)| *w)
        .unwrap_or(1)
}

/// Determine the credit weight for an execute action based on its resource/action.
pub fn execute_action_weight(resource: &str, action: &str) -> i64 {
    // Check if the action matches a known heavy category
    for &(pattern, weight) in EXECUTE_WEIGHTS {
        if action.contains(pattern) || resource.contains(pattern) {
            return weight;
        }
    }
    // Default execute weight: 3 (investigate/recommend class)
    3
}

/// Check if the organization has available credits before executing.
/// Returns `Some(reason)` if denied, `None` if allowed.
///
/// Only enforces hard caps on free tier (orgs without a Stripe subscription).
/// Paid tiers always pass because overage is billed via Stripe graduated pricing.
pub async fn check_credit_allowance(ctx: &ActionContext) -> Option<String> {
    let db = ctx.db.as_ref()?;
    let org_id = ctx.organization_id?;

    // Get the tier config from the cached entitlements service
    let tier = ctx.entitlements.get_config(org_id).await.ok()?;
    let limit = tier.config.gateway.agent_credits_included;

    // -1 means unlimited
    if limit < 0 {
        return None;
    }

    // Only enforce for free tier (no active subscription)
    let has_subscription: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM stripe_subscriptions WHERE organization_id = $1 AND status IN ('active', 'trialing'))"
    )
    .bind(org_id)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    if has_subscription {
        return None;
    }

    let billing_start = {
        use chrono::{Datelike, Utc};
        let now = Utc::now();
        now.with_day(1)
            .unwrap_or(now)
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    };

    let used: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(credits), 0) FROM mcp_credit_log WHERE organization_id = $1 AND created_at >= $2"
    )
    .bind(org_id)
    .bind(billing_start)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    if used >= limit {
        return Some(format!(
            "Credit limit reached: {used}/{limit} credits used this billing period. Upgrade to continue."
        ));
    }

    None
}

/// Emit a credit metering event after a successful action execution.
///
/// Does two things:
/// 1. Writes to `mcp_credit_log` in Postgres for free-tier enforcement queries.
/// 2. Sends a Stripe billing meter event for paid-tier overage billing.
///
/// Only emits if the context has a meter_service and organization_id.
/// The idempotency key is derived from the tool name and a unique execution ID.
pub fn emit_credit_event(ctx: &ActionContext, tool_name: &str, arguments: &serde_json::Value, execution_id: Uuid) {
    let meter = match &ctx.meter_service {
        Some(m) => m,
        None => {
            debug!("No meter service configured, skipping credit metering");
            return;
        }
    };

    let org_id = match ctx.organization_id {
        Some(id) => id,
        None => {
            warn!(
                project_id = %ctx.project_id,
                tool_name = %tool_name,
                "Cannot emit credit event: organization_id not set on ActionContext"
            );
            return;
        }
    };

    let weight = if tool_name == "execute" {
        let resource = arguments.get("resource").and_then(|v| v.as_str()).unwrap_or("");
        let action = arguments.get("action").and_then(|v| v.as_str()).unwrap_or("");
        execute_action_weight(resource, action)
    } else {
        credit_weight(tool_name)
    };

    let idempotency_key = format!("mcp-{}-{}", tool_name, execution_id);

    // Write to Postgres credit log for enforcement queries
    if let Some(db) = &ctx.db {
        let db = db.clone();
        let project_id = ctx.project_id;
        let tool = tool_name.to_string();
        let key = idempotency_key.clone();
        tokio::spawn(async move {
            if let Err(e) = sqlx::query(
                "INSERT INTO mcp_credit_log (organization_id, project_id, tool_name, credits, idempotency_key)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (idempotency_key) DO NOTHING"
            )
            .bind(org_id)
            .bind(project_id)
            .bind(&tool)
            .bind(weight as i32)
            .bind(&key)
            .execute(&db)
            .await
            {
                error!(
                    organization_id = %org_id,
                    tool_name = %tool,
                    error = %e,
                    "Failed to write mcp_credit_log entry"
                );
            }
        });
    }

    meter.record_credits(org_id, weight, idempotency_key);

    debug!(
        organization_id = %org_id,
        tool_name = %tool_name,
        credits = weight,
        "Emitted credit metering event"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credit_weight_returns_correct_values() {
        assert_eq!(credit_weight("search"), 1);
        assert_eq!(credit_weight("get"), 1);
        assert_eq!(credit_weight("list"), 1);
        assert_eq!(credit_weight("analyze"), 2);
        assert_eq!(credit_weight("execute"), 3);
    }

    #[test]
    fn credit_weight_unknown_tool_defaults_to_1() {
        assert_eq!(credit_weight("unknown_tool"), 1);
        assert_eq!(credit_weight(""), 1);
    }

    #[test]
    fn execute_action_weight_maps_correctly() {
        // Default execute actions (investigate/recommend class)
        assert_eq!(execute_action_weight("dashboard", "create"), 3);
        assert_eq!(execute_action_weight("alert_rule", "update"), 3);
        assert_eq!(execute_action_weight("prompt", "create_version"), 3);

        // Report-class actions
        assert_eq!(execute_action_weight("prompt", "report"), 5);
        assert_eq!(execute_action_weight("project", "export"), 5);
        assert_eq!(execute_action_weight("project", "generate"), 5);

        // Compiler
        assert_eq!(execute_action_weight("prompt", "compile"), 10);
        assert_eq!(execute_action_weight("compiler", "run"), 10);
    }

    #[test]
    fn idempotency_key_format_is_stable() {
        let execution_id = Uuid::nil();
        let key = format!("mcp-{}-{}", "execute", execution_id);
        assert_eq!(
            key,
            "mcp-execute-00000000-0000-0000-0000-000000000000"
        );
    }
}
