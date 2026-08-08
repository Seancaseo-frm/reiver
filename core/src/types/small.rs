//! Stack-allocated small strings and vectors for reducing heap allocations.
//!
//! These types use inline storage for small data, falling back to heap allocation
//! only when the inline capacity is exceeded. This reduces allocator pressure
//! and improves cache locality for common cases.
//!
//! # Usage Guidelines
//!
//! Use these types for:
//! - Fixed-size or predictably small data (trace IDs, span IDs, status codes)
//! - Short-lived intermediate collections
//! - Data that is frequently cloned
//!
//! Do NOT use for:
//! - ClickHouse insert structs (require `String` for serde)
//! - Long content (request/response bodies, log messages)
//! - User-provided values with unknown sizes
//!
//! Note: These utilities are available for use in hot paths but may not all
//! be currently active in the codebase.

#![allow(dead_code)]

use smallstr::SmallString;
use smallvec::SmallVec;

// =============================================================================
// SmallString Type Aliases
// =============================================================================

/// Trace ID type - 32 hex characters (128-bit trace ID formatted as hex).
/// Stored inline on the stack, avoiding heap allocation.
pub type TraceIdStr = SmallString<[u8; 32]>;

/// Span ID type - 16 hex characters (64-bit span ID formatted as hex).
/// Stored inline on the stack, avoiding heap allocation.
pub type SpanIdStr = SmallString<[u8; 16]>;

/// Status code string - for values like "STATUS_CODE_OK", "STATUS_CODE_ERROR".
/// 24 bytes covers all standard OTLP status codes.
pub type StatusStr = SmallString<[u8; 24]>;

/// Short string for provider names, service names, etc.
/// Covers common names like "openai", "anthropic", "google".
pub type ShortStr = SmallString<[u8; 24]>;

/// Attribute key string - most OTLP attribute keys are under 48 chars.
/// Examples: "gen_ai.request.temperature", "deployment.environment".
pub type AttrKeyStr = SmallString<[u8; 48]>;

// =============================================================================
// SmallVec Type Aliases
// =============================================================================

/// Attribute vector - for storing key-value pairs.
/// Inline capacity of 16 covers most spans/logs/metrics without heap allocation.
/// Based on typical OTLP attribute counts: 5-30 attributes per entity.
pub type AttrVec = SmallVec<[(String, String); 16]>;

/// Filter values vector - for small filter sets.
/// Inline capacity of 8 covers most filter operations.
pub type FilterVec = SmallVec<[(String, String); 8]>;

/// Small string vector - for short lists of strings.
/// Inline capacity of 8 for things like tag lists, severity levels.
pub type SmallStrVec = SmallVec<[String; 8]>;

// =============================================================================
// Helper Functions
// =============================================================================

/// Format a 16-byte trace ID as a 32-character hex string using SmallString.
/// Avoids heap allocation for the common case.
#[inline]
pub fn format_trace_id_small(bytes: &[u8]) -> TraceIdStr {
    use std::fmt::Write;
    let mut s = TraceIdStr::new();
    if bytes.len() == 16 {
        // Fast path for standard 16-byte trace IDs
        let id = u128::from_be_bytes(bytes.try_into().unwrap_or([0u8; 16]));
        write!(&mut s, "{:032x}", id).ok();
    } else {
        // Fallback for non-standard lengths
        for b in bytes {
            write!(&mut s, "{:02x}", b).ok();
        }
    }
    s
}

/// Format an 8-byte span ID as a 16-character hex string using SmallString.
/// Avoids heap allocation for the common case.
#[inline]
pub fn format_span_id_small(bytes: &[u8]) -> SpanIdStr {
    use std::fmt::Write;
    let mut s = SpanIdStr::new();
    if bytes.len() == 8 {
        // Fast path for standard 8-byte span IDs
        let id = u64::from_be_bytes(bytes.try_into().unwrap_or([0u8; 8]));
        write!(&mut s, "{:016x}", id).ok();
    } else {
        // Fallback for non-standard lengths
        for b in bytes {
            write!(&mut s, "{:02x}", b).ok();
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_format() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let trace_id = format_trace_id_small(&bytes);
        assert_eq!(trace_id.as_str(), "0123456789abcdeffedcba9876543210");
        // SmallString with 32-byte capacity fits a 32-char trace ID inline
        assert_eq!(trace_id.len(), 32);
    }

    #[test]
    fn test_span_id_format() {
        let bytes = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
        let span_id = format_span_id_small(&bytes);
        assert_eq!(span_id.as_str(), "0123456789abcdef");
        // SmallString with 16-byte capacity fits a 16-char span ID inline
        assert_eq!(span_id.len(), 16);
    }

    #[test]
    fn test_attr_vec_inline() {
        let mut attrs: AttrVec = SmallVec::new();

        // Add 15 items - should stay inline
        for i in 0..15 {
            attrs.push((format!("key{}", i), format!("value{}", i)));
        }

        // SmallVec uses spilled() to check if heap-allocated
        assert!(!attrs.spilled());
        assert!(attrs.len() <= 16);
    }

    #[test]
    fn test_status_str_inline() {
        let status: StatusStr = SmallString::from_str("STATUS_CODE_ERROR");
        assert_eq!(status.as_str(), "STATUS_CODE_ERROR");
        // 17 chars fits within 24-byte capacity
        assert_eq!(status.len(), 17);
    }
}
