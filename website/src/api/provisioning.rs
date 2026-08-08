//! Provisioning Rules API
//!
//! JIT (Just-In-Time) provisioning rules for automatic user role and project assignment
//! based on SSO attributes like groups, email domain, or custom claims.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};
use crate::auth::extract_user_id;
use crate::authorization::require_org_admin;
use crate::error::{AppError, Result};

// ============================================================================
// Authorization Helpers
// ============================================================================

/// Get the organization_id for a rule and verify admin access
async fn require_rule_admin(db: &sqlx::PgPool, user_id: Uuid, rule_id: Uuid) -> Result<Uuid> {
    let rule: Option<(Uuid,)> =
        sqlx::query_as("SELECT organization_id FROM provisioning_rules WHERE id = $1")
            .bind(rule_id)
            .fetch_optional(db)
            .await
            .map_err(|e| {
                error!("Failed to get provisioning rule: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error"))
            })?;

    let org_id = rule
        .ok_or_else(|| AppError::NotFound("Provisioning rule not found".to_string()))?
        .0;

    require_org_admin(db, user_id, org_id).await?;
    Ok(org_id)
}

pub fn create_provisioning_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/rules", get(list_rules).post(create_rule))
        .route(
            "/rules/{rule_id}",
            get(get_rule).put(update_rule).delete(delete_rule),
        )
        .route("/rules/evaluate", post(evaluate_rules))
        .route("/rules/reorder", post(reorder_rules))
}

// ============================================================================
// Types
// ============================================================================

/// Provisioning rule stored in database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ProvisioningRule {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub sso_config_id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub priority: i32,
    pub enabled: bool,
    pub condition: serde_json::Value,
    pub actions: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Rule condition types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleCondition {
    /// Always matches
    Always,
    /// Match if user is in any of the specified groups
    GroupMembership {
        groups: Vec<String>,
        #[serde(default)]
        match_all: bool,
    },
    /// Match if email matches a pattern
    EmailPattern {
        pattern: String, // e.g., "*@engineering.example.com"
    },
    /// Match if email domain is in list
    EmailDomain { domains: Vec<String> },
    /// Match if a specific attribute has a value
    AttributeMatch {
        attribute: String,
        value: String,
        #[serde(default)]
        operator: MatchOperator,
    },
    /// Match if all conditions are true
    And { conditions: Vec<RuleCondition> },
    /// Match if any condition is true
    Or { conditions: Vec<RuleCondition> },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MatchOperator {
    #[default]
    Equals,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
}

/// Actions to take when rule matches
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleAction {
    /// Assign a role to the user
    AssignRole {
        role: String, // 'admin', 'member', 'viewer'
    },
    /// Add user to projects
    AddToProjects { project_ids: Vec<Uuid> },
    /// Add user to projects by name pattern
    AddToProjectsByPattern { pattern: String },
    /// Set a user attribute
    SetAttribute { key: String, value: String },
}

/// Request to create a new rule
#[derive(Debug, Deserialize)]
pub struct CreateRuleRequest {
    pub organization_id: Uuid,
    pub sso_config_id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub priority: Option<i32>,
    pub enabled: Option<bool>,
    pub condition: serde_json::Value,
    pub actions: serde_json::Value,
}

/// Request to update a rule
#[derive(Debug, Deserialize)]
pub struct UpdateRuleRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub priority: Option<i32>,
    pub enabled: Option<bool>,
    pub condition: Option<serde_json::Value>,
    pub actions: Option<serde_json::Value>,
}

/// Query parameters for listing rules
#[derive(Debug, Deserialize)]
pub struct ListRulesParams {
    pub organization_id: Uuid,
    pub sso_config_id: Option<Uuid>,
    pub enabled_only: Option<bool>,
}

/// Context for evaluating rules
#[derive(Debug, Deserialize)]
pub struct EvaluationContext {
    pub organization_id: Uuid,
    pub sso_config_id: Option<Uuid>,
    pub email: String,
    pub groups: Option<Vec<String>>,
    pub attributes: Option<serde_json::Value>,
}

/// Result of rule evaluation
#[derive(Debug, Serialize)]
pub struct EvaluationResult {
    pub matched_rules: Vec<MatchedRule>,
    pub computed_role: Option<String>,
    pub computed_projects: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct MatchedRule {
    pub rule_id: Uuid,
    pub rule_name: String,
    pub actions: serde_json::Value,
}

// ============================================================================
// Endpoints
// ============================================================================

/// List provisioning rules for an organization
async fn list_rules(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Query(params): Query<ListRulesParams>,
) -> Result<Json<Vec<ProvisioningRule>>> {
    // Require authentication and admin access to the organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_org_admin(&state.db, user_id, params.organization_id).await?;

    let enabled_only = params.enabled_only.unwrap_or(false);

    let rules = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        SELECT id, organization_id, sso_config_id, name, description,
               priority, enabled, condition, actions, created_at, updated_at
        FROM provisioning_rules
        WHERE organization_id = $1
          AND ($2::uuid IS NULL OR sso_config_id = $2)
          AND ($3 = false OR enabled = true)
        ORDER BY priority ASC, created_at ASC
        "#,
    )
    .bind(params.organization_id)
    .bind(params.sso_config_id)
    .bind(enabled_only)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to list provisioning rules: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    Ok(Json(rules))
}

/// Create a new provisioning rule
async fn create_rule(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<CreateRuleRequest>,
) -> Result<Json<ProvisioningRule>> {
    // Require authentication and admin access to the organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_org_admin(&state.db, user_id, req.organization_id).await?;

    // Validate condition and actions JSON
    if let Err(e) = serde_json::from_value::<RuleCondition>(req.condition.clone()) {
        return Err(AppError::Validation(format!("Invalid condition: {}", e)));
    }

    let actions: Vec<serde_json::Value> = serde_json::from_value(req.actions.clone())
        .map_err(|e| AppError::Validation(format!("Actions must be an array: {}", e)))?;

    for action in &actions {
        if let Err(e) = serde_json::from_value::<RuleAction>(action.clone()) {
            return Err(AppError::Validation(format!("Invalid action: {}", e)));
        }
    }

    let rule = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        INSERT INTO provisioning_rules (
            organization_id, sso_config_id, name, description,
            priority, enabled, condition, actions
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, organization_id, sso_config_id, name, description,
                  priority, enabled, condition, actions, created_at, updated_at
        "#,
    )
    .bind(req.organization_id)
    .bind(req.sso_config_id)
    .bind(&req.name)
    .bind(&req.description)
    .bind(req.priority.unwrap_or(100))
    .bind(req.enabled.unwrap_or(true))
    .bind(&req.condition)
    .bind(&req.actions)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create provisioning rule: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error: {}", e))
    })?;

    info!("Created provisioning rule: {} ({})", rule.name, rule.id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ProvisioningRuleCreated)
        .actor(user_id)
        .organization(req.organization_id)
        .resource("provisioning_rule", rule.id)
        .details(serde_json::json!({ "created": {
            "name": &rule.name,
            "enabled": rule.enabled,
            "condition": &rule.condition,
            "actions": &rule.actions,
        }}))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(rule))
}

/// Get a specific rule
async fn get_rule(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
) -> Result<Json<ProvisioningRule>> {
    // Require authentication and admin access to the rule's organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_rule_admin(&state.db, user_id, rule_id).await?;

    let rule = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        SELECT id, organization_id, sso_config_id, name, description,
               priority, enabled, condition, actions, created_at, updated_at
        FROM provisioning_rules
        WHERE id = $1
        "#,
    )
    .bind(rule_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Rule not found".to_string()))?;

    Ok(Json(rule))
}

/// Update a provisioning rule
async fn update_rule(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
    Json(req): Json<UpdateRuleRequest>,
) -> Result<Json<ProvisioningRule>> {
    // Require authentication and admin access to the rule's organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_rule_admin(&state.db, user_id, rule_id).await?;

    // Validate condition if provided
    if let Some(ref condition) = req.condition {
        if let Err(e) = serde_json::from_value::<RuleCondition>(condition.clone()) {
            return Err(AppError::Validation(format!("Invalid condition: {}", e)));
        }
    }

    // Validate actions if provided
    if let Some(ref actions) = req.actions {
        let actions_vec: Vec<serde_json::Value> = serde_json::from_value(actions.clone())
            .map_err(|e| AppError::Validation(format!("Actions must be an array: {}", e)))?;

        for action in &actions_vec {
            if let Err(e) = serde_json::from_value::<RuleAction>(action.clone()) {
                return Err(AppError::Validation(format!("Invalid action: {}", e)));
            }
        }
    }

    let before: Option<ProvisioningRule> = sqlx::query_as(
        "SELECT id, organization_id, sso_config_id, name, description, priority, enabled, condition, actions, created_at, updated_at FROM provisioning_rules WHERE id = $1"
    )
    .bind(rule_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let rule = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        UPDATE provisioning_rules
        SET name = COALESCE($1, name),
            description = COALESCE($2, description),
            priority = COALESCE($3, priority),
            enabled = COALESCE($4, enabled),
            condition = COALESCE($5, condition),
            actions = COALESCE($6, actions),
            updated_at = NOW()
        WHERE id = $7
        RETURNING id, organization_id, sso_config_id, name, description,
                  priority, enabled, condition, actions, created_at, updated_at
        "#,
    )
    .bind(req.name.as_deref())
    .bind(req.description.as_deref())
    .bind(req.priority)
    .bind(req.enabled)
    .bind(&req.condition)
    .bind(&req.actions)
    .bind(rule_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Rule not found".to_string()))?;

    info!("Updated provisioning rule: {} ({})", rule.name, rule.id);

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::ProvisioningRuleUpdated)
        .actor(user_id)
        .organization(rule.organization_id)
        .resource("provisioning_rule", rule.id)
        .details(serde_json::json!({
            "before": {
                "name": before.as_ref().map(|b| &b.name),
                "enabled": before.as_ref().map(|b| b.enabled),
                "condition": before.as_ref().map(|b| &b.condition),
                "actions": before.as_ref().map(|b| &b.actions),
            },
            "after": {
                "name": &rule.name,
                "enabled": rule.enabled,
                "condition": &rule.condition,
                "actions": &rule.actions,
            },
        }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(rule))
}

/// Delete a provisioning rule
async fn delete_rule(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
) -> Result<StatusCode> {
    // Require authentication and admin access to the rule's organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_rule_admin(&state.db, user_id, rule_id).await?;

    let before: Option<ProvisioningRule> = sqlx::query_as(
        "SELECT id, organization_id, sso_config_id, name, description, priority, enabled, condition, actions, created_at, updated_at FROM provisioning_rules WHERE id = $1"
    )
    .bind(rule_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let result = sqlx::query("DELETE FROM provisioning_rules WHERE id = $1")
        .bind(rule_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Rule not found".to_string()));
    }

    info!("Deleted provisioning rule: {}", rule_id);

    let org_id = before.as_ref().map(|b| b.organization_id);
    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    let mut builder = AuditEventBuilder::new(AuditEventType::ProvisioningRuleDeleted)
        .actor(user_id)
        .resource("provisioning_rule", rule_id)
        .details(serde_json::json!({ "deleted": {
            "name": before.as_ref().map(|b| &b.name),
            "enabled": before.as_ref().map(|b| b.enabled),
            "condition": before.as_ref().map(|b| &b.condition),
            "actions": before.as_ref().map(|b| &b.actions),
        }}))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success();
    if let Some(org_id) = org_id {
        builder = builder.organization(org_id);
    }
    builder.log(&state.clickhouse).await;

    Ok(StatusCode::NO_CONTENT)
}

/// Reorder rules
#[derive(Debug, Deserialize)]
pub struct ReorderRequest {
    pub organization_id: Uuid,
    pub rule_ids: Vec<Uuid>,
}

async fn reorder_rules(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(req): Json<ReorderRequest>,
) -> Result<StatusCode> {
    // Require authentication and admin access to the organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_org_admin(&state.db, user_id, req.organization_id).await?;

    // Update priorities based on order in the list
    for (index, rule_id) in req.rule_ids.iter().enumerate() {
        sqlx::query(
            "UPDATE provisioning_rules SET priority = $1, updated_at = NOW() WHERE id = $2",
        )
        .bind(index as i32)
        .bind(rule_id)
        .execute(&*state.db)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Evaluate rules against a context (for testing/preview)
async fn evaluate_rules(
    State(state): State<Arc<WebsiteState>>,
    headers: HeaderMap,
    Json(ctx): Json<EvaluationContext>,
) -> Result<Json<EvaluationResult>> {
    // Require authentication and admin access to the organization
    let user_id = extract_user_id(&headers, &state.config.jwt_secret)?;
    require_org_admin(&state.db, user_id, ctx.organization_id).await?;

    // Get all enabled rules for the organization
    let rules = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        SELECT id, organization_id, sso_config_id, name, description,
               priority, enabled, condition, actions, created_at, updated_at
        FROM provisioning_rules
        WHERE organization_id = $1
          AND ($2::uuid IS NULL OR sso_config_id IS NULL OR sso_config_id = $2)
          AND enabled = true
        ORDER BY priority ASC
        "#,
    )
    .bind(ctx.organization_id)
    .bind(ctx.sso_config_id)
    .fetch_all(&*state.db)
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Database error: {}", e)))?;

    let mut matched_rules = Vec::new();
    let mut computed_role: Option<String> = None;
    let mut computed_projects: Vec<Uuid> = Vec::new();

    for rule in rules {
        let condition: RuleCondition =
            serde_json::from_value(rule.condition.clone()).unwrap_or(RuleCondition::Always);

        if evaluate_condition(&condition, &ctx) {
            matched_rules.push(MatchedRule {
                rule_id: rule.id,
                rule_name: rule.name.clone(),
                actions: rule.actions.clone(),
            });

            // Apply actions
            let actions: Vec<RuleAction> =
                serde_json::from_value(rule.actions.clone()).unwrap_or_default();

            for action in actions {
                match action {
                    RuleAction::AssignRole { role } => {
                        computed_role = Some(role);
                    }
                    RuleAction::AddToProjects { project_ids } => {
                        for pid in project_ids {
                            if !computed_projects.contains(&pid) {
                                computed_projects.push(pid);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(Json(EvaluationResult {
        matched_rules,
        computed_role,
        computed_projects,
    }))
}

// ============================================================================
// Condition Evaluation
// ============================================================================

/// Maximum compiled regex size to prevent ReDoS attacks
const MAX_REGEX_SIZE: usize = 10_000;
/// Maximum DFA size for regex to prevent memory exhaustion
const MAX_DFA_SIZE: usize = 10_000;

/// Safely create a regex with size limits to prevent ReDoS attacks.
/// Returns None if the regex is invalid or exceeds size limits.
fn safe_regex(pattern: &str) -> Option<regex::Regex> {
    regex::RegexBuilder::new(pattern)
        .size_limit(MAX_REGEX_SIZE)
        .dfa_size_limit(MAX_DFA_SIZE)
        .build()
        .ok()
}

fn evaluate_condition(condition: &RuleCondition, ctx: &EvaluationContext) -> bool {
    match condition {
        RuleCondition::Always => true,

        RuleCondition::GroupMembership { groups, match_all } => {
            if let Some(user_groups) = &ctx.groups {
                if *match_all {
                    groups.iter().all(|g| user_groups.contains(g))
                } else {
                    groups.iter().any(|g| user_groups.contains(g))
                }
            } else {
                false
            }
        }

        RuleCondition::EmailPattern { pattern } => {
            // SECURITY: Use safe_regex with size limits to prevent ReDoS
            let regex_pattern = pattern.replace('.', "\\.").replace('*', ".*");
            safe_regex(&format!("^{}$", regex_pattern))
                .map(|re| re.is_match(&ctx.email))
                .unwrap_or(false)
        }

        RuleCondition::EmailDomain { domains } => ctx
            .email
            .split('@')
            .last()
            .map(|domain| domains.iter().any(|d| d.eq_ignore_ascii_case(domain)))
            .unwrap_or(false),

        RuleCondition::AttributeMatch {
            attribute,
            value,
            operator,
        } => {
            if let Some(attrs) = &ctx.attributes {
                if let Some(attr_value) = attrs.get(attribute).and_then(|v| v.as_str()) {
                    match operator {
                        MatchOperator::Equals => attr_value == value,
                        MatchOperator::Contains => attr_value.contains(value.as_str()),
                        MatchOperator::StartsWith => attr_value.starts_with(value.as_str()),
                        MatchOperator::EndsWith => attr_value.ends_with(value.as_str()),
                        MatchOperator::Regex => {
                            // SECURITY: Use safe_regex with size limits to prevent ReDoS
                            safe_regex(value)
                                .map(|re| re.is_match(attr_value))
                                .unwrap_or(false)
                        }
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }

        RuleCondition::And { conditions } => conditions.iter().all(|c| evaluate_condition(c, ctx)),

        RuleCondition::Or { conditions } => conditions.iter().any(|c| evaluate_condition(c, ctx)),
    }
}

// ============================================================================
// Public API for SSO flow
// ============================================================================

/// Apply provisioning rules to a newly authenticated user
pub async fn apply_provisioning_rules(
    db: &sqlx::PgPool,
    organization_id: Uuid,
    sso_config_id: Uuid,
    user_id: Uuid,
    email: &str,
    groups: &[String],
    attributes: Option<&serde_json::Value>,
) -> anyhow::Result<(String, Vec<Uuid>)> {
    let ctx = EvaluationContext {
        organization_id,
        sso_config_id: Some(sso_config_id),
        email: email.to_string(),
        groups: Some(groups.to_vec()),
        attributes: attributes.cloned(),
    };

    // Get enabled rules
    let rules = sqlx::query_as::<_, ProvisioningRule>(
        r#"
        SELECT id, organization_id, sso_config_id, name, description,
               priority, enabled, condition, actions, created_at, updated_at
        FROM provisioning_rules
        WHERE organization_id = $1
          AND (sso_config_id IS NULL OR sso_config_id = $2)
          AND enabled = true
        ORDER BY priority ASC
        "#,
    )
    .bind(organization_id)
    .bind(sso_config_id)
    .fetch_all(db)
    .await?;

    let mut role = "member".to_string(); // Default role
    let mut project_ids: Vec<Uuid> = Vec::new();

    for rule in rules {
        let condition: RuleCondition =
            serde_json::from_value(rule.condition.clone()).unwrap_or(RuleCondition::Always);

        if evaluate_condition(&condition, &ctx) {
            let actions: Vec<RuleAction> =
                serde_json::from_value(rule.actions.clone()).unwrap_or_default();

            for action in actions {
                match action {
                    RuleAction::AssignRole { role: r } => {
                        role = r;
                    }
                    RuleAction::AddToProjects { project_ids: pids } => {
                        for pid in pids {
                            if !project_ids.contains(&pid) {
                                project_ids.push(pid);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Update user role
    sqlx::query("UPDATE users SET role = $1 WHERE id = $2")
        .bind(&role)
        .bind(user_id)
        .execute(db)
        .await?;

    // Add user to projects (implementation depends on project_members table)
    // This is a simplified version - you may need to adjust based on actual schema
    for project_id in &project_ids {
        sqlx::query(
            r#"
            INSERT INTO project_members (project_id, user_id, role)
            VALUES ($1, $2, 'member')
            ON CONFLICT (project_id, user_id) DO NOTHING
            "#,
        )
        .bind(project_id)
        .bind(user_id)
        .execute(db)
        .await
        .ok();
    }

    info!(
        "Applied provisioning rules for user {}: role={}, projects={:?}",
        user_id, role, project_ids
    );
    Ok((role, project_ids))
}
