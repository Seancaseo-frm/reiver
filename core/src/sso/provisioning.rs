//! JIT (Just-In-Time) Provisioning Engine
//!
//! Provides rule-based user provisioning based on SSO claims.
//! Rules can:
//! - Assign users to projects based on group membership
//! - Set roles based on attributes
//! - Block users based on conditions

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, info};
use uuid::Uuid;

/// A provisioning rule
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ProvisioningRule {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub sso_config_id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    /// Priority (lower = higher priority)
    pub priority: i32,
    /// Whether the rule is enabled
    pub enabled: bool,
    /// Condition (JSON)
    #[sqlx(json)]
    pub condition: RuleCondition,
    /// Actions to take when condition matches (JSON)
    #[sqlx(json)]
    pub actions: Vec<ProvisioningAction>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Condition for rule matching
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleCondition {
    /// Always matches
    Always,
    /// Never matches
    Never,
    /// Match if email domain matches
    EmailDomain { domain: String },
    /// Match if user has specific group
    HasGroup { group: String },
    /// Match if attribute has value
    AttributeEquals { attribute: String, value: String },
    /// Match if attribute contains value
    AttributeContains { attribute: String, value: String },
    /// All conditions must match
    And { conditions: Vec<RuleCondition> },
    /// Any condition must match
    Or { conditions: Vec<RuleCondition> },
    /// Condition must not match
    Not { condition: Box<RuleCondition> },
}

impl Default for RuleCondition {
    fn default() -> Self {
        RuleCondition::Always
    }
}

/// Action to take when a rule matches
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProvisioningAction {
    /// Assign user to a project with a role
    AssignProject { project_id: Uuid, role: String },
    /// Set user's default role
    SetRole { role: String },
    /// Add user to a group (internal)
    AddToGroup { group_name: String },
    /// Block the user from logging in
    Block { reason: String },
    /// Set a user attribute
    SetAttribute { key: String, value: String },
}

/// User claims from SSO (input to the provisioning engine)
#[derive(Debug, Clone)]
pub struct UserClaims {
    pub email: String,
    pub name: Option<String>,
    pub groups: Vec<String>,
    pub attributes: std::collections::HashMap<String, String>,
}

/// Result of provisioning evaluation
#[derive(Debug, Clone, Default)]
pub struct ProvisioningResult {
    /// Projects to assign with roles
    pub project_assignments: Vec<(Uuid, String)>,
    /// Default role to set
    pub role: Option<String>,
    /// Groups to add to
    pub groups: Vec<String>,
    /// Whether to block the user
    pub blocked: bool,
    /// Block reason if blocked
    pub block_reason: Option<String>,
    /// Additional attributes to set
    pub attributes: std::collections::HashMap<String, String>,
    /// Rules that matched
    pub matched_rules: Vec<Uuid>,
}

/// Provisioning engine
pub struct ProvisioningEngine<'a> {
    db: &'a PgPool,
}

impl<'a> ProvisioningEngine<'a> {
    pub fn new(db: &'a PgPool) -> Self {
        Self { db }
    }

    /// Evaluate provisioning rules for a user
    pub async fn evaluate(
        &self,
        organization_id: Uuid,
        sso_config_id: Option<Uuid>,
        claims: &UserClaims,
    ) -> Result<ProvisioningResult> {
        // Fetch rules ordered by priority
        let rules = self.get_rules(organization_id, sso_config_id).await?;

        let mut result = ProvisioningResult::default();

        for rule in rules {
            if !rule.enabled {
                continue;
            }

            if self.evaluate_condition(&rule.condition, claims) {
                // SECURITY: Don't log email (PII) - log only the rule name
                debug!("Provisioning rule '{}' matched", rule.name);
                result.matched_rules.push(rule.id);

                for action in &rule.actions {
                    self.apply_action(action, &mut result);

                    // If blocked, stop processing
                    if result.blocked {
                        return Ok(result);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get provisioning rules for an organization
    async fn get_rules(
        &self,
        organization_id: Uuid,
        sso_config_id: Option<Uuid>,
    ) -> Result<Vec<ProvisioningRule>> {
        let rules = sqlx::query_as::<_, ProvisioningRule>(
            r#"
            SELECT * FROM provisioning_rules
            WHERE organization_id = $1
              AND (sso_config_id IS NULL OR sso_config_id = $2)
              AND enabled = true
            ORDER BY priority ASC, created_at ASC
            "#,
        )
        .bind(organization_id)
        .bind(sso_config_id)
        .fetch_all(self.db)
        .await
        .context("Failed to fetch provisioning rules")?;

        Ok(rules)
    }

    /// Evaluate a condition against user claims
    fn evaluate_condition(&self, condition: &RuleCondition, claims: &UserClaims) -> bool {
        evaluate_condition(condition, claims)
    }
}

/// Evaluate a condition against user claims (standalone function for testability)
pub fn evaluate_condition(condition: &RuleCondition, claims: &UserClaims) -> bool {
    match condition {
        RuleCondition::Always => true,
        RuleCondition::Never => false,

        RuleCondition::EmailDomain { domain } => claims.email.ends_with(&format!("@{}", domain)),

        RuleCondition::HasGroup { group } => {
            claims.groups.iter().any(|g| g.eq_ignore_ascii_case(group))
        }

        RuleCondition::AttributeEquals { attribute, value } => claims
            .attributes
            .get(attribute)
            .map(|v| v == value)
            .unwrap_or(false),

        RuleCondition::AttributeContains { attribute, value } => claims
            .attributes
            .get(attribute)
            .map(|v| v.to_lowercase().contains(&value.to_lowercase()))
            .unwrap_or(false),

        RuleCondition::And { conditions } => {
            conditions.iter().all(|c| evaluate_condition(c, claims))
        }

        RuleCondition::Or { conditions } => {
            conditions.iter().any(|c| evaluate_condition(c, claims))
        }

        RuleCondition::Not { condition } => !evaluate_condition(condition, claims),
    }
}

impl<'a> ProvisioningEngine<'a> {
    /// Apply an action to the result
    fn apply_action(&self, action: &ProvisioningAction, result: &mut ProvisioningResult) {
        match action {
            ProvisioningAction::AssignProject { project_id, role } => {
                result.project_assignments.push((*project_id, role.clone()));
            }

            ProvisioningAction::SetRole { role } => {
                result.role = Some(role.clone());
            }

            ProvisioningAction::AddToGroup { group_name } => {
                if !result.groups.contains(group_name) {
                    result.groups.push(group_name.clone());
                }
            }

            ProvisioningAction::Block { reason } => {
                result.blocked = true;
                result.block_reason = Some(reason.clone());
            }

            ProvisioningAction::SetAttribute { key, value } => {
                result.attributes.insert(key.clone(), value.clone());
            }
        }
    }

    /// Create a new provisioning rule
    pub async fn create_rule(
        &self,
        organization_id: Uuid,
        sso_config_id: Option<Uuid>,
        name: &str,
        description: Option<&str>,
        priority: i32,
        condition: RuleCondition,
        actions: Vec<ProvisioningAction>,
    ) -> Result<ProvisioningRule> {
        let rule = sqlx::query_as::<_, ProvisioningRule>(
            r#"
            INSERT INTO provisioning_rules (
                organization_id, sso_config_id, name, description,
                priority, condition, actions
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            "#,
        )
        .bind(organization_id)
        .bind(sso_config_id)
        .bind(name)
        .bind(description)
        .bind(priority)
        .bind(sqlx::types::Json(&condition))
        .bind(sqlx::types::Json(&actions))
        .fetch_one(self.db)
        .await
        .context("Failed to create provisioning rule")?;

        info!(
            "Created provisioning rule '{}' for org {}",
            name, organization_id
        );

        Ok(rule)
    }

    /// Update a provisioning rule
    pub async fn update_rule(
        &self,
        rule_id: Uuid,
        name: Option<&str>,
        description: Option<&str>,
        priority: Option<i32>,
        enabled: Option<bool>,
        condition: Option<RuleCondition>,
        actions: Option<Vec<ProvisioningAction>>,
    ) -> Result<ProvisioningRule> {
        let rule = sqlx::query_as::<_, ProvisioningRule>(
            r#"
            UPDATE provisioning_rules
            SET name = COALESCE($2, name),
                description = COALESCE($3, description),
                priority = COALESCE($4, priority),
                enabled = COALESCE($5, enabled),
                condition = COALESCE($6, condition),
                actions = COALESCE($7, actions),
                updated_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(rule_id)
        .bind(name)
        .bind(description)
        .bind(priority)
        .bind(enabled)
        .bind(condition.map(|c| sqlx::types::Json(c)))
        .bind(actions.map(|a| sqlx::types::Json(a)))
        .fetch_one(self.db)
        .await
        .context("Failed to update provisioning rule")?;

        Ok(rule)
    }

    /// Delete a provisioning rule
    pub async fn delete_rule(&self, rule_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM provisioning_rules WHERE id = $1")
            .bind(rule_id)
            .execute(self.db)
            .await
            .context("Failed to delete provisioning rule")?;

        info!("Deleted provisioning rule {}", rule_id);
        Ok(())
    }

    /// List all rules for an organization
    pub async fn list_rules(&self, organization_id: Uuid) -> Result<Vec<ProvisioningRule>> {
        let rules = sqlx::query_as::<_, ProvisioningRule>(
            r#"
            SELECT * FROM provisioning_rules
            WHERE organization_id = $1
            ORDER BY priority ASC, created_at ASC
            "#,
        )
        .bind(organization_id)
        .fetch_all(self.db)
        .await
        .context("Failed to list provisioning rules")?;

        Ok(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claims() -> UserClaims {
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("department".to_string(), "Engineering".to_string());

        UserClaims {
            email: "user@example.com".to_string(),
            name: Some("Test User".to_string()),
            groups: vec!["developers".to_string(), "admins".to_string()],
            attributes,
        }
    }

    #[test]
    fn test_condition_always() {
        let claims = test_claims();

        assert!(evaluate_condition(&RuleCondition::Always, &claims));
        assert!(!evaluate_condition(&RuleCondition::Never, &claims));
    }

    #[test]
    fn test_condition_email_domain() {
        let claims = test_claims();

        assert!(evaluate_condition(
            &RuleCondition::EmailDomain {
                domain: "example.com".to_string()
            },
            &claims
        ));
        assert!(!evaluate_condition(
            &RuleCondition::EmailDomain {
                domain: "other.com".to_string()
            },
            &claims
        ));
    }

    #[test]
    fn test_condition_has_group() {
        let claims = test_claims();

        assert!(evaluate_condition(
            &RuleCondition::HasGroup {
                group: "developers".to_string()
            },
            &claims
        ));
        assert!(evaluate_condition(
            &RuleCondition::HasGroup {
                group: "ADMINS".to_string()
            }, // Case insensitive
            &claims
        ));
        assert!(!evaluate_condition(
            &RuleCondition::HasGroup {
                group: "managers".to_string()
            },
            &claims
        ));
    }

    #[test]
    fn test_condition_and_or() {
        let claims = test_claims();

        // Both true
        assert!(evaluate_condition(
            &RuleCondition::And {
                conditions: vec![
                    RuleCondition::HasGroup {
                        group: "developers".to_string()
                    },
                    RuleCondition::EmailDomain {
                        domain: "example.com".to_string()
                    },
                ]
            },
            &claims
        ));

        // One false
        assert!(!evaluate_condition(
            &RuleCondition::And {
                conditions: vec![
                    RuleCondition::HasGroup {
                        group: "developers".to_string()
                    },
                    RuleCondition::HasGroup {
                        group: "managers".to_string()
                    },
                ]
            },
            &claims
        ));

        // Or - one true
        assert!(evaluate_condition(
            &RuleCondition::Or {
                conditions: vec![
                    RuleCondition::HasGroup {
                        group: "managers".to_string()
                    },
                    RuleCondition::HasGroup {
                        group: "developers".to_string()
                    },
                ]
            },
            &claims
        ));
    }

    #[test]
    fn test_condition_not() {
        let claims = test_claims();

        assert!(evaluate_condition(
            &RuleCondition::Not {
                condition: Box::new(RuleCondition::HasGroup {
                    group: "managers".to_string()
                })
            },
            &claims
        ));
    }
}
