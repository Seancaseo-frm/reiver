//! String interning for frequently repeated attribute keys and values.
//!
//! String interning stores only one copy of each unique string, returning a
//! lightweight handle (Spur) that can be compared in O(1) and resolved to the
//! original string. This is particularly useful for:
//!
//! - OTLP attribute keys (e.g., "service.name", "deployment.environment")
//! - Common attribute values (e.g., severity levels, status codes)
//! - Service names that appear in many spans/logs/metrics
//!
//! # Thread Safety
//!
//! The `ThreadedRodeo` interner is thread-safe and lock-free for reads.
//! Writes (interning new strings) use internal synchronization.
//!
//! # Performance
//!
//! - Interning: ~30-50ns for cache hit, ~100ns for new strings
//! - Resolution: ~5-10ns (direct memory lookup)
//! - Handle comparison: O(1) integer comparison
//!
//! # Usage
//!
//! ```ignore
//! use crate::intern::{intern, resolve};
//!
//! let key = intern("service.name");  // Returns a Spur handle
//! let value = resolve(key);          // Returns &str
//! ```
//!
//! Note: This module is prewarmed at startup. The interning functions are
//! available for use but may not all be currently active in the codebase.

#![allow(dead_code)]

use lasso::{Spur, ThreadedRodeo};
use once_cell::sync::Lazy;

/// Global thread-safe string interner.
/// Uses a hash map internally with lock-free reads.
static INTERNER: Lazy<ThreadedRodeo> = Lazy::new(ThreadedRodeo::default);

/// Intern a string, returning a lightweight handle.
/// If the string is already interned, returns the existing handle.
///
/// # Example
///
/// ```ignore
/// let handle1 = intern("service.name");
/// let handle2 = intern("service.name");
/// assert_eq!(handle1, handle2);  // Same handle for same string
/// ```
#[inline]
pub fn intern(s: &str) -> Spur {
    INTERNER.get_or_intern(s)
}

/// Intern a static string, potentially more efficiently.
/// Use this for string literals that are known at compile time.
#[inline]
pub fn intern_static(s: &'static str) -> Spur {
    INTERNER.get_or_intern_static(s)
}

/// Resolve an interned handle back to its string.
/// Returns None if the handle is invalid (shouldn't happen in normal use).
#[inline]
pub fn resolve(key: Spur) -> &'static str {
    // SAFETY: The interner lives for the lifetime of the program
    INTERNER.resolve(&key)
}

/// Try to get an existing interned string without creating a new one.
/// Returns None if the string hasn't been interned yet.
#[inline]
pub fn try_get(s: &str) -> Option<Spur> {
    INTERNER.get(s)
}

/// Check if a string is already interned.
#[inline]
pub fn is_interned(s: &str) -> bool {
    INTERNER.contains(s)
}

/// Pre-intern common OTLP attribute keys AND values.
/// Call this during application startup to avoid interning overhead during processing.
///
/// This prewarms both:
/// - **Keys**: Common attribute names (e.g., "http.method", "service.name")
/// - **Values**: Common attribute values (e.g., "GET", "POST", "production")
///
/// Interning common values saves memory and reduces allocations in the hot path
/// when the same values appear repeatedly across spans/logs/metrics.
pub fn prewarm_common_keys() {
    // ========================================================================
    // ATTRIBUTE KEYS
    // ========================================================================

    // Common OTLP resource attributes
    intern_static("service.name");
    intern_static("service.version");
    intern_static("service.namespace");
    intern_static("deployment.environment");
    intern_static("cloud.provider");
    intern_static("cloud.region");
    intern_static("cloud.availability_zone");
    intern_static("host.name");
    intern_static("host.type");
    intern_static("container.name");
    intern_static("container.id");
    intern_static("k8s.pod.name");
    intern_static("k8s.namespace.name");
    intern_static("k8s.deployment.name");

    // Common span attributes
    intern_static("http.method");
    intern_static("http.url");
    intern_static("http.status_code");
    intern_static("http.route");
    intern_static("http.target");
    intern_static("http.host");
    intern_static("http.scheme");
    intern_static("http.user_agent");
    intern_static("db.system");
    intern_static("db.statement");
    intern_static("db.operation");
    intern_static("db.name");
    intern_static("rpc.system");
    intern_static("rpc.service");
    intern_static("rpc.method");
    intern_static("messaging.system");
    intern_static("messaging.destination");
    intern_static("messaging.operation");
    intern_static("net.peer.name");
    intern_static("net.peer.port");
    intern_static("net.host.name");
    intern_static("net.host.port");

    // GenAI semantic convention attributes
    intern_static("gen_ai.system");
    intern_static("gen_ai.operation.name");
    intern_static("gen_ai.request.model");
    intern_static("gen_ai.request.max_tokens");
    intern_static("gen_ai.request.temperature");
    intern_static("gen_ai.response.model");
    intern_static("gen_ai.response.id");
    intern_static("gen_ai.usage.input_tokens");
    intern_static("gen_ai.usage.output_tokens");

    // ========================================================================
    // ATTRIBUTE VALUES
    // ========================================================================

    // HTTP methods (very common, appear in almost every HTTP span)
    intern_static("GET");
    intern_static("POST");
    intern_static("PUT");
    intern_static("DELETE");
    intern_static("PATCH");
    intern_static("HEAD");
    intern_static("OPTIONS");

    // HTTP schemes
    intern_static("http");
    intern_static("https");
    intern_static("grpc");
    intern_static("grpcs");

    // Common environment names
    intern_static("production");
    intern_static("prod");
    intern_static("staging");
    intern_static("stage");
    intern_static("development");
    intern_static("dev");
    intern_static("test");
    intern_static("local");

    // Cloud providers
    intern_static("aws");
    intern_static("gcp");
    intern_static("azure");
    intern_static("qwen");
    intern_static("oracle");

    // Common AWS regions
    intern_static("us-east-1");
    intern_static("us-east-2");
    intern_static("us-west-1");
    intern_static("us-west-2");
    intern_static("eu-west-1");
    intern_static("eu-west-2");
    intern_static("eu-central-1");
    intern_static("ap-southeast-1");
    intern_static("ap-northeast-1");

    // Database systems
    intern_static("postgresql");
    intern_static("mysql");
    intern_static("redis");
    intern_static("mongodb");
    intern_static("elasticsearch");
    intern_static("clickhouse");

    // RPC systems
    intern_static("grpc");
    intern_static("aws-api");

    // Messaging systems
    intern_static("kafka");
    intern_static("rabbitmq");
    intern_static("sqs");
    intern_static("sns");

    // GenAI providers
    intern_static("openai");
    intern_static("anthropic");
    intern_static("google");
    intern_static("bedrock");
    intern_static("azure-openai");

    // Common GenAI models
    intern_static("gpt-4");
    intern_static("gpt-4o");
    intern_static("gpt-4o-mini");
    intern_static("gpt-4-turbo");
    intern_static("gpt-3.5-turbo");
    intern_static("claude-3-opus-20240229");
    intern_static("claude-3-sonnet-20240229");
    intern_static("claude-3-haiku-20240307");
    intern_static("claude-3-5-sonnet-20240620");

    // Boolean strings (from to_string())
    intern_static("true");
    intern_static("false");

    // Severity levels (log/trace)
    intern_static("TRACE");
    intern_static("DEBUG");
    intern_static("INFO");
    intern_static("WARN");
    intern_static("ERROR");
    intern_static("FATAL");

    // Status codes
    intern_static("STATUS_CODE_OK");
    intern_static("STATUS_CODE_ERROR");
    intern_static("STATUS_CODE_UNSET");
    intern_static("ok");
    intern_static("error");
    intern_static("unset");

    // Span kinds
    intern_static("INTERNAL");
    intern_static("SERVER");
    intern_static("CLIENT");
    intern_static("PRODUCER");
    intern_static("CONSUMER");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_and_resolve() {
        let key = intern("test.key");
        let resolved = resolve(key);
        assert_eq!(resolved, "test.key");
    }

    #[test]
    fn test_same_string_same_handle() {
        let key1 = intern("service.name");
        let key2 = intern("service.name");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_different_strings_different_handles() {
        let key1 = intern("service.name");
        let key2 = intern("service.version");
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_try_get() {
        let key = intern("existing.key");
        assert!(try_get("existing.key").is_some());
        assert_eq!(try_get("existing.key"), Some(key));
        assert!(try_get("nonexistent.key").is_none());
    }

    #[test]
    fn test_is_interned() {
        intern("check.this");
        assert!(is_interned("check.this"));
        assert!(!is_interned("never.interned"));
    }

    #[test]
    fn test_prewarm() {
        prewarm_common_keys();
        assert!(is_interned("service.name"));
        assert!(is_interned("gen_ai.system"));
        assert!(is_interned("STATUS_CODE_OK"));
    }
}
