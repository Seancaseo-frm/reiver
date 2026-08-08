//! PII detection and redaction with static `OnceLock` regexes.
//!
//! All patterns are compiled exactly once per process (on first use) and reused
//! for every subsequent call. This is safe to call millions of times on hot
//! paths — zero regex compilation after startup.
//!
//! Detection priority (highest first, to prevent overlapping spans):
//! 1. Email  2. CreditCard (Luhn-validated)  3. SSN  4. Phone (NANP)
//! 5. Phone (E.164 international)  6. IPv4

use bb8_redis::redis::AsyncCommands;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

use crate::app_state::RedisPool;
use crate::db::DbPool;

const PII_CACHE_TTL_SECS: u64 = 300;
const MIN_PII_LEN: usize = 8;

// ── Static regex singletons (compiled once per process) ──────────────────────

fn email_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b").unwrap())
}

fn credit_card_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\b(?:(?:4\d{3}|5[1-5]\d{2}|6(?:011|5\d{2}))[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}|3[47]\d{2}[- ]?\d{6}[- ]?\d{5})\b",
        ).unwrap()
    })
}

fn ssn_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap())
}

fn phone_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b(?:\+?1[\s.\-]?)?\(?[2-9]\d{2}\)?[\s.\-]\d{3}[\s.\-]\d{4}\b").unwrap()
    })
}

fn intl_phone_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\+[1-9]\d{9,13}\b|\+[1-9]\d{0,3}[\s.\-]\d{2,9}(?:[\s.\-]\d{2,9}){0,4}\b")
            .unwrap()
    })
}

fn ipv4_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b",
        )
        .unwrap()
    })
}

// ── PII types ────────────────────────────────────────────────────────────────

/// PII category detected in a text value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PiiType {
    Email,
    CreditCard,
    Ssn,
    Phone,
    IpAddress,
}

impl std::fmt::Display for PiiType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PiiType::Email => write!(f, "Email"),
            PiiType::CreditCard => write!(f, "CreditCard"),
            PiiType::Ssn => write!(f, "SSN"),
            PiiType::Phone => write!(f, "Phone"),
            PiiType::IpAddress => write!(f, "IpAddress"),
        }
    }
}

// ── Span detection ───────────────────────────────────────────────────────────

struct PiiSpan {
    start: usize,
    end: usize,
    pii_type: PiiType,
}

/// SSA format validation: area numbers 000, 666, and 900-999 are invalid.
fn is_valid_ssn(s: &str) -> bool {
    if let Some(area) = s.get(0..3) {
        if let Ok(n) = area.parse::<u16>() {
            return n != 0 && n != 666 && n < 900;
        }
    }
    false
}

/// Luhn checksum validation for credit card numbers.
fn is_valid_cc(s: &str) -> bool {
    let digits_only: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    luhn::valid(&digits_only)
}

/// Find all non-overlapping PII spans in `text`, ordered by start offset.
fn find_spans(text: &str) -> Vec<PiiSpan> {
    let mut spans: Vec<PiiSpan> = Vec::new();
    let mut occupied = Vec::new(); // (start, end) of already-claimed ranges

    macro_rules! push_regex {
        ($re:expr, $pii_type:expr) => {
            push_regex!($re, $pii_type, |_: &str| true)
        };
        ($re:expr, $pii_type:expr, $validate:expr) => {
            for m in $re.find_iter(text) {
                let start = m.start();
                let end = m.end();
                if occupied.iter().any(|&(s, e)| start < e && end > s) {
                    continue;
                }
                let matched = m.as_str();
                let validate: fn(&str) -> bool = $validate;
                if !validate(matched) {
                    continue;
                }
                occupied.push((start, end));
                spans.push(PiiSpan {
                    start,
                    end,
                    pii_type: $pii_type,
                });
            }
        };
    }

    push_regex!(email_re(), PiiType::Email);
    push_regex!(credit_card_re(), PiiType::CreditCard, is_valid_cc);
    push_regex!(ssn_re(), PiiType::Ssn, is_valid_ssn);
    push_regex!(phone_re(), PiiType::Phone);
    push_regex!(intl_phone_re(), PiiType::Phone);
    push_regex!(ipv4_re(), PiiType::IpAddress);

    spans.sort_by_key(|s| s.start);
    spans
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Detect which PII types are present in `text`.
///
/// Returns a deduplicated list of PII categories found. Used by the warehouse
/// PII scanner to classify columns.
pub fn detect_pii(text: &str) -> Vec<PiiType> {
    if text.len() < MIN_PII_LEN {
        return Vec::new();
    }
    let spans = find_spans(text);
    let mut seen = HashSet::new();
    spans
        .into_iter()
        .filter_map(|s| {
            if seen.insert(s.pii_type.clone()) {
                Some(s.pii_type)
            } else {
                None
            }
        })
        .collect()
}

/// Redact PII in `text`, returning `Some(redacted)` when PII was found,
/// or `None` when the text is clean (zero-allocation fast path for callers).
pub fn redact_if_changed(text: &str) -> Option<String> {
    if text.len() < MIN_PII_LEN {
        return None;
    }
    let spans = find_spans(text);
    if spans.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    for span in &spans {
        out.push_str(&text[cursor..span.start]);
        match span.pii_type {
            PiiType::Email => out.push_str("[EMAIL]"),
            PiiType::CreditCard => out.push_str("[CARD]"),
            PiiType::Ssn => out.push_str("[SSN]"),
            PiiType::Phone => out.push_str("[PHONE]"),
            PiiType::IpAddress => out.push_str("[IP]"),
        }
        cursor = span.end;
    }
    out.push_str(&text[cursor..]);
    Some(out)
}

/// Mask PII in a string, returning `Cow::Borrowed` when nothing changed.
pub fn mask_pii(s: &str) -> Cow<'_, str> {
    match redact_if_changed(s) {
        Some(redacted) => Cow::Owned(redacted),
        None => Cow::Borrowed(s),
    }
}

/// Reads projects.settings->>'pii_masking_enabled'. Returns true when missing or not set (mask by default).
pub async fn get_pii_masking_enabled(db: &DbPool, project_id: Uuid) -> bool {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT COALESCE((settings->>'pii_masking_enabled')::boolean, true) FROM projects WHERE id = $1"
    )
    .bind(project_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.map(|r| r.0).unwrap_or(true)
}

/// Cached version of `get_pii_masking_enabled` using Redis.
///
/// Reduces database round-trips for high-volume ingestion endpoints (OTLP, logs).
/// Cache TTL is 5 minutes.
pub async fn get_pii_masking_enabled_cached(
    redis: &RedisPool,
    db: &DbPool,
    project_id: Uuid,
) -> bool {
    let cache_key = format!("pii_enabled:{}", project_id);

    if let Ok(mut conn) = redis.get().await {
        let cached: Option<String> =
            tokio::time::timeout(Duration::from_secs(1), conn.get(&cache_key))
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten();

        if let Some(value) = cached {
            debug!("PII masking cache hit for project_id={}", project_id);
            return value == "1";
        }
    }

    debug!(
        "PII masking cache miss for project_id={}, querying database",
        project_id
    );
    let enabled = get_pii_masking_enabled(db, project_id).await;

    if let Ok(mut conn) = redis.get().await {
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            conn.set_ex::<_, _, ()>(
                &cache_key,
                if enabled { "1" } else { "0" },
                PII_CACHE_TTL_SECS,
            ),
        )
        .await;
    }

    enabled
}

/// Invalidate the PII masking cache for a project (call when settings change).
pub async fn invalidate_pii_cache(redis: &RedisPool, project_id: Uuid) {
    let cache_key = format!("pii_enabled:{}", project_id);

    if let Ok(mut conn) = redis.get().await {
        let _ = conn.del::<_, ()>(&cache_key).await;
        debug!(
            "Invalidated PII masking cache for project_id={}",
            project_id
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_ssn() {
        let result = redact_if_changed("My SSN is 123-45-6789 on file");
        assert!(result.is_some(), "SSN should be detected");
        let redacted = result.unwrap();
        assert!(!redacted.contains("123-45-6789"), "SSN should be redacted");
        assert!(redacted.contains("[SSN]"));
    }

    #[test]
    fn rejects_invalid_ssn_area() {
        assert!(redact_if_changed("Number 000-12-3456 here").is_none());
        assert!(redact_if_changed("Number 666-12-3456 here").is_none());
        assert!(redact_if_changed("Number 900-12-3456 here").is_none());
    }

    #[test]
    fn masks_credit_card() {
        let result = redact_if_changed("Card 4111111111111111 charged");
        assert!(result.is_some(), "Credit card should be detected");
        let redacted = result.unwrap();
        assert!(
            !redacted.contains("4111111111111111"),
            "CC should be redacted"
        );
        assert!(redacted.contains("[CARD]"));
    }

    #[test]
    fn rejects_invalid_luhn() {
        assert!(redact_if_changed("Card 4111111111111112 charged").is_none());
    }

    #[test]
    fn masks_email() {
        let result = redact_if_changed("Contact user@example.com for help");
        assert!(result.is_some(), "Email should be detected");
        let redacted = result.unwrap();
        assert!(
            !redacted.contains("user@example.com"),
            "Email should be redacted"
        );
        assert!(redacted.contains("[EMAIL]"));
    }

    #[test]
    fn masks_ipv4() {
        let result = redact_if_changed("Server at 192.168.1.100 is down");
        assert!(result.is_some(), "IPv4 should be detected");
        let redacted = result.unwrap();
        assert!(!redacted.contains("192.168.1.100"));
        assert!(redacted.contains("[IP]"));
    }

    #[test]
    fn masks_mixed_patterns() {
        let text = "SSN is 123-45-6789 and email is john@example.com";
        let result = redact_if_changed(text);
        assert!(result.is_some(), "Mixed PII should be detected");
        let redacted = result.unwrap();
        assert!(!redacted.contains("123-45-6789"));
        assert!(!redacted.contains("john@example.com"));
    }

    #[test]
    fn clean_text_returns_none() {
        assert!(redact_if_changed("Connection failed to db:5432").is_none());
    }

    #[test]
    fn empty_string_returns_none() {
        assert!(redact_if_changed("").is_none());
    }

    #[test]
    fn short_string_returns_none() {
        assert!(redact_if_changed("ok").is_none());
    }

    #[test]
    fn mask_pii_cow_borrowed_when_clean() {
        let s = "Connection failed to db:5432";
        assert!(matches!(mask_pii(s), Cow::Borrowed(_)));
    }

    #[test]
    fn mask_pii_cow_owned_when_pii() {
        let result = mask_pii("My SSN is 123-45-6789 on file");
        assert!(matches!(result, Cow::Owned(_)));
        assert!(!result.contains("123-45-6789"));
    }

    #[test]
    fn detect_pii_returns_types() {
        let types = detect_pii("Email: alice@corp.com SSN: 123-45-6789");
        assert!(types.contains(&PiiType::Email));
        assert!(types.contains(&PiiType::Ssn));
    }

    #[test]
    fn detect_pii_empty_on_clean() {
        assert!(detect_pii("Connection failed to db:5432").is_empty());
    }

    #[test]
    fn regex_singletons_are_reused() {
        let a = email_re() as *const Regex;
        let b = email_re() as *const Regex;
        assert_eq!(a, b, "OnceLock singleton must return same pointer");
    }

    #[test]
    fn no_overlapping_spans() {
        let text = "user@example.com is an email";
        let spans = find_spans(text);
        for window in spans.windows(2) {
            assert!(window[0].end <= window[1].start, "spans must not overlap");
        }
    }
}
