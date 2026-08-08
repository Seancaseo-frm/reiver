/// Public/free email providers that should not be used for domain-based org matching.
const PUBLIC_EMAIL_DOMAINS: &[&str] = &[
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "yahoo.co.uk",
    "yahoo.co.jp",
    "ymail.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "protonmail.com",
    "proton.me",
    "pm.me",
    "mail.com",
    "zoho.com",
    "yandex.com",
    "yandex.ru",
    "gmx.com",
    "gmx.net",
    "fastmail.com",
    "tutanota.com",
    "tuta.com",
    "qq.com",
    "163.com",
    "126.com",
    "sina.com",
    "att.net",
    "comcast.net",
    "verizon.net",
    "hey.com",
    "duck.com",
    "mailbox.org",
];

/// Returns true if the given domain is a well-known public/free email provider.
pub fn is_public_email_domain(domain: &str) -> bool {
    PUBLIC_EMAIL_DOMAINS.contains(&domain.to_lowercase().as_str())
}

/// Extracts the domain part from an email address.
/// Returns `None` if the email has no `@` or the domain is empty.
pub fn extract_email_domain(email: &str) -> Option<&str> {
    let parts: Vec<&str> = email.rsplitn(2, '@').collect();
    if parts.len() == 2 && !parts[0].is_empty() {
        Some(parts[0])
    } else {
        None
    }
}

/// Extracts the domain from an email if it's a company (non-public) domain.
/// Returns `None` for public email providers or invalid emails.
pub fn extract_company_domain(email: &str) -> Option<&str> {
    extract_email_domain(email).filter(|d| !is_public_email_domain(d))
}

/// Local part of `user@domain` (single `@`, non-empty both sides).
pub fn extract_email_local_part(email: &str) -> Option<&str> {
    let parts: Vec<&str> = email.rsplitn(2, '@').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Some(parts[1])
    } else {
        None
    }
}

/// Max length for `organizations.name` (Postgres `VARCHAR(255)`).
pub const MAX_ORGANIZATION_NAME_LEN: usize = 255;

fn clamp_org_name(s: &str) -> String {
    s.chars().take(MAX_ORGANIZATION_NAME_LEN).collect()
}

/// Personal (consumer email) default org name: sanitized local-part + `'s workspace`,
/// or `"Personal workspace"` when the local-part is missing or only symbols.
pub fn personal_workspace_name_from_email(email: &str) -> String {
    const FALLBACK: &str = "Personal workspace";
    let local = extract_email_local_part(email)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(local) = local else {
        return clamp_org_name(FALLBACK);
    };
    let sanitized: String = local
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '_' || *c == '-')
        .take(200)
        .collect();
    if sanitized.is_empty() {
        return clamp_org_name(FALLBACK);
    }
    clamp_org_name(&format!("{sanitized}'s workspace"))
}

/// Company email: name is the domain string (lowercased); `domain` is set for `organizations.domain`.
/// Personal email: name from [`personal_workspace_name_from_email`]; `domain` is `None`.
pub fn suggested_org_provision_from_email(email: &str) -> (String, Option<String>) {
    if let Some(d) = extract_company_domain(email) {
        let label = clamp_org_name(&d.to_lowercase());
        (label.clone(), Some(label))
    } else {
        (personal_workspace_name_from_email(email), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_domains() {
        assert!(is_public_email_domain("gmail.com"));
        assert!(is_public_email_domain("Gmail.COM"));
        assert!(is_public_email_domain("outlook.com"));
        assert!(!is_public_email_domain("reiver.ai"));
        assert!(!is_public_email_domain("acme.co"));
    }

    #[test]
    fn test_extract_domain() {
        assert_eq!(extract_email_domain("user@acme.com"), Some("acme.com"));
        assert_eq!(extract_email_domain("user@gmail.com"), Some("gmail.com"));
        assert_eq!(extract_email_domain("nodomain"), None);
    }

    #[test]
    fn test_extract_company_domain() {
        assert_eq!(extract_company_domain("user@acme.com"), Some("acme.com"));
        assert_eq!(extract_company_domain("user@gmail.com"), None);
    }

    #[test]
    fn test_extract_email_local_part() {
        assert_eq!(extract_email_local_part("alex@acme.com"), Some("alex"));
        assert_eq!(extract_email_local_part("nodomain"), None);
    }

    #[test]
    fn test_personal_workspace_name() {
        assert_eq!(
            personal_workspace_name_from_email("alex@gmail.com"),
            "alex's workspace"
        );
        assert_eq!(
            personal_workspace_name_from_email("a.b_c-d@yahoo.com"),
            "a.b_c-d's workspace"
        );
        assert_eq!(
            personal_workspace_name_from_email("@@@gmail.com"),
            "Personal workspace"
        );
    }

    #[test]
    fn test_suggested_org_provision_from_email() {
        let (name, dom) = suggested_org_provision_from_email("billing@Acme.COM");
        assert_eq!(name, "acme.com");
        assert_eq!(dom.as_deref(), Some("acme.com"));

        let (name, dom) = suggested_org_provision_from_email("me@gmail.com");
        assert_eq!(dom, None);
        assert_eq!(name, "me's workspace");
    }
}
