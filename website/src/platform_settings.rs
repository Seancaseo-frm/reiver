//! Platform-wide settings stored in `platform_settings` (Postgres).

use reiver_core::db::DbPool;

/// Returns the `is_approved` flag for **self-serve account registration** only:
/// email/password sign-up and cold OAuth sign-up (new account, no invite).
///
/// This setting does **not** apply to users provisioned via SCIM or SSO into an
/// existing organization — those flows set `is_approved` explicitly.
///
/// When the stored `require_signup_approval` is `true`, new self-serve users start
/// as not approved; when `false`, they are approved immediately.
/// Invite-link and domain-invite OAuth paths always create approved users.
pub async fn self_serve_signup_is_approved(db: &DbPool) -> Result<bool, sqlx::Error> {
    let v: Option<String> = sqlx::query_scalar(
        "SELECT value FROM platform_settings WHERE key = 'require_signup_approval'",
    )
    .fetch_optional(db)
    .await?;

    Ok(match v.as_deref() {
        Some("false") => true,
        _ => false,
    })
}
