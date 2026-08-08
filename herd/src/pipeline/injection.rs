//! Prompt injection detection for A2A message parts.
//!
//! Delegates to `reiver_core::prompt_injection` for pattern matching,
//! normalization (typoglycemia, spaced/repeated chars), Base64 decoding,
//! and special token detection.

use crate::a2a::types::Part;

/// Detect prompt injection in message parts. Returns true if any pattern matches.
pub fn detect_injection(parts: &[Part]) -> bool {
    for part in parts {
        if let Some(ref text) = part.text {
            if let Some(detail) = reiver_core::prompt_injection::detect(text) {
                tracing::warn!(detail, "Prompt injection detected in message part");
                return true;
            }
        }
    }
    false
}
