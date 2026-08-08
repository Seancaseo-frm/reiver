//! PII scrubbing for A2A message parts.
//!
//! Delegates to `reiver_core::pii` for detection and redaction, giving Herd
//! the same coverage as Flow and Pond: Luhn-validated credit cards, SSN area
//! validation, international phone numbers, IPv4 addresses, and non-overlapping
//! span logic.

use crate::a2a::types::Part;

/// Scrub PII from message parts in-place. Returns true if any redaction occurred.
pub fn scrub_message_parts(parts: &mut [Part]) -> bool {
    let mut redacted = false;
    for part in parts.iter_mut() {
        if let Some(ref mut text) = part.text {
            if let Some(clean) = reiver_core::pii::redact_if_changed(text) {
                *text = clean;
                redacted = true;
            }
        }
        if let Some(ref mut data) = part.data {
            if let Some(s) = data.as_str() {
                if let Some(clean) = reiver_core::pii::redact_if_changed(s) {
                    *data = serde_json::Value::String(clean);
                    redacted = true;
                }
            }
        }
    }
    redacted
}
