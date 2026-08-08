//! Default organization name + domain when auto-creating an org for a user.

use uuid::Uuid;

use crate::db::DbPool;
use crate::domains::{personal_workspace_name_from_email, suggested_org_provision_from_email};

/// Resolved `organizations.name` / `organizations.domain` for a new org, plus email for retries.
#[derive(Debug, Clone)]
pub struct DefaultOrgProvision {
    pub user_email: String,
    pub suggested_name: String,
    pub domain: Option<String>,
}

impl DefaultOrgProvision {
    /// Use when `INSERT` hits unique(`organizations.domain`): same user, personal name, no domain.
    pub fn fallback_without_company_domain(&self) -> Self {
        Self {
            user_email: self.user_email.clone(),
            suggested_name: personal_workspace_name_from_email(&self.user_email),
            domain: None,
        }
    }
}

/// Loads the user's email and applies company-vs-personal rules (see `domains::suggested_org_provision_from_email`).
pub async fn default_org_provision_for_user(
    pool: &DbPool,
    user_id: Uuid,
) -> Result<DefaultOrgProvision, sqlx::Error> {
    let user_email: String = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(pool)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

    let (suggested_name, domain) = suggested_org_provision_from_email(&user_email);

    Ok(DefaultOrgProvision {
        user_email,
        suggested_name,
        domain,
    })
}
