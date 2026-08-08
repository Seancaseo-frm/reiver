//! Scope constants and helpers for token permissions.
//!
//! Seven coarse scopes grouped by product area. A `:write` scope
//! implies the corresponding `:read` scope for the same area.

pub const PROJECT_READ: &str = "project:read";
pub const PROJECT_WRITE: &str = "project:write";
pub const LLM_READ: &str = "llm:read";
pub const LLM_WRITE: &str = "llm:write";
pub const OBSERVABILITY_READ: &str = "observability:read";
pub const OBSERVABILITY_WRITE: &str = "observability:write";
pub const WAREHOUSE_READ: &str = "warehouse:read";
pub const WAREHOUSE_WRITE: &str = "warehouse:write";
pub const HERD_READ: &str = "herd:read";
pub const HERD_WRITE: &str = "herd:write";
pub const BILLING_READ: &str = "billing:read";

// Internal-only scopes — NOT in ALL_SCOPES so users cannot create API keys with them.
// Injected programmatically by the agent-task handler when internal=true.
pub const INTERNAL_READ: &str = "internal:read";
pub const INTERNAL_WRITE: &str = "internal:write";

pub const ALL_SCOPES: &[&str] = &[
    PROJECT_READ,
    PROJECT_WRITE,
    LLM_READ,
    LLM_WRITE,
    OBSERVABILITY_READ,
    OBSERVABILITY_WRITE,
    WAREHOUSE_READ,
    WAREHOUSE_WRITE,
    HERD_READ,
    HERD_WRITE,
    BILLING_READ,
];

pub const READ_ONLY_SCOPES: &[&str] = &[
    PROJECT_READ,
    LLM_READ,
    OBSERVABILITY_READ,
    WAREHOUSE_READ,
    HERD_READ,
    BILLING_READ,
];

/// If `scope` is a write scope, return the implied read scope.
pub fn write_implies_read(scope: &str) -> Option<&'static str> {
    match scope {
        PROJECT_WRITE => Some(PROJECT_READ),
        LLM_WRITE => Some(LLM_READ),
        OBSERVABILITY_WRITE => Some(OBSERVABILITY_READ),
        WAREHOUSE_WRITE => Some(WAREHOUSE_READ),
        HERD_WRITE => Some(HERD_READ),
        INTERNAL_WRITE => Some(INTERNAL_READ),
        _ => None,
    }
}

/// Check whether `granted` scopes satisfy a `required` scope.
///
/// A write scope implicitly grants the corresponding read scope,
/// e.g. `llm:write` satisfies a `llm:read` requirement.
pub fn has_scope(granted: &[String], required: &str) -> bool {
    if granted.iter().any(|s| s == required) {
        return true;
    }
    // Check if any granted write scope implies the required read scope
    for g in granted {
        if let Some(implied_read) = write_implies_read(g) {
            if implied_read == required {
                return true;
            }
        }
    }
    false
}

/// Return the maximum scopes a user role is allowed to create tokens with.
pub fn role_max_scopes(role: &str) -> Vec<&'static str> {
    match role {
        "owner" | "admin" => ALL_SCOPES.to_vec(),
        "member" => vec![
            PROJECT_READ,
            LLM_READ,
            LLM_WRITE,
            OBSERVABILITY_READ,
            OBSERVABILITY_WRITE,
            WAREHOUSE_READ,
            WAREHOUSE_WRITE,
            HERD_READ,
            HERD_WRITE,
            BILLING_READ,
        ],
        "viewer" => READ_ONLY_SCOPES.to_vec(),
        _ => Vec::new(),
    }
}

/// Validate that every requested scope is within the ceiling allowed by the user's role.
pub fn validate_scopes_within_ceiling(requested: &[String], role: &str) -> Result<(), String> {
    let max = role_max_scopes(role);
    for scope in requested {
        if !max.contains(&scope.as_str()) {
            return Err(format!(
                "Scope '{}' exceeds permissions for role '{}'. Allowed: {:?}",
                scope, role, max
            ));
        }
    }
    Ok(())
}

/// Check that every string in `scopes` is a valid scope name.
pub fn validate_scope_names(scopes: &[String]) -> Result<(), String> {
    for scope in scopes {
        if !ALL_SCOPES.contains(&scope.as_str()) {
            return Err(format!(
                "Unknown scope '{}'. Valid scopes: {:?}",
                scope, ALL_SCOPES
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_implies_read_check() {
        let granted = vec!["llm:write".to_string()];
        assert!(has_scope(&granted, "llm:write"));
        assert!(has_scope(&granted, "llm:read"));
        assert!(!has_scope(&granted, "project:read"));
    }

    #[test]
    fn direct_read_match() {
        let granted = vec!["project:read".to_string(), "billing:read".to_string()];
        assert!(has_scope(&granted, "project:read"));
        assert!(has_scope(&granted, "billing:read"));
        assert!(!has_scope(&granted, "project:write"));
    }

    #[test]
    fn ceiling_enforcement() {
        assert!(validate_scopes_within_ceiling(
            &["project:read".into(), "llm:read".into()],
            "viewer"
        )
        .is_ok());

        assert!(validate_scopes_within_ceiling(&["project:write".into()], "viewer").is_err());

        assert!(validate_scopes_within_ceiling(&["project:write".into()], "admin").is_ok());

        assert!(validate_scopes_within_ceiling(&["project:write".into()], "member").is_err());
    }

    #[test]
    fn scope_name_validation() {
        assert!(validate_scope_names(&["llm:read".into()]).is_ok());
        assert!(validate_scope_names(&["invalid:scope".into()]).is_err());
    }

    #[test]
    fn warehouse_write_implies_read() {
        let granted = vec!["warehouse:write".to_string()];
        assert!(has_scope(&granted, "warehouse:write"));
        assert!(has_scope(&granted, "warehouse:read"));
        assert!(!has_scope(&granted, "llm:read"));
    }

    #[test]
    fn warehouse_scopes_are_valid() {
        assert!(validate_scope_names(&["warehouse:read".into()]).is_ok());
        assert!(validate_scope_names(&["warehouse:write".into()]).is_ok());
    }

    #[test]
    fn warehouse_read_in_read_only_scopes() {
        assert!(READ_ONLY_SCOPES.contains(&"warehouse:read"));
        assert!(!READ_ONLY_SCOPES.contains(&"warehouse:write"));
    }

    #[test]
    fn member_has_warehouse_scopes() {
        let member_scopes = role_max_scopes("member");
        assert!(member_scopes.contains(&"warehouse:read"));
        assert!(member_scopes.contains(&"warehouse:write"));
    }

    #[test]
    fn herd_write_implies_read() {
        let granted = vec!["herd:write".to_string()];
        assert!(has_scope(&granted, "herd:write"));
        assert!(has_scope(&granted, "herd:read"));
        assert!(!has_scope(&granted, "llm:read"));
    }

    #[test]
    fn herd_scopes_are_valid() {
        assert!(validate_scope_names(&["herd:read".into()]).is_ok());
        assert!(validate_scope_names(&["herd:write".into()]).is_ok());
    }

    #[test]
    fn herd_read_in_read_only_scopes() {
        assert!(READ_ONLY_SCOPES.contains(&"herd:read"));
        assert!(!READ_ONLY_SCOPES.contains(&"herd:write"));
    }

    #[test]
    fn member_has_herd_scopes() {
        let member_scopes = role_max_scopes("member");
        assert!(member_scopes.contains(&"herd:read"));
        assert!(member_scopes.contains(&"herd:write"));
    }
}
