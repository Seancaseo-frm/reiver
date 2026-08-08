//! Hot Path Benchmarks
//!
//! Benchmarks for performance-critical code paths that run on every request or message.
//!
//! Run with: cargo bench --bench hot_path_benchmarks
//!
//! Run specific benchmark groups:
//!   cargo bench --bench hot_path_benchmarks -- fingerprint
//!   cargo bench --bench hot_path_benchmarks -- json_parsing
//!   cargo bench --bench hot_path_benchmarks -- rate_limit_keys
//!
//! These benchmarks cover:
//!
//! **Fingerprint Generation:**
//! - Message normalization with varying message lengths
//! - Full fingerprint generation with different stack trace depths
//!
//! **JSON Parsing:**
//! - serde_json vs simd-json performance comparison
//! - Various payload sizes (small, medium, large)
//!
//! **Rate Limit Key Formatting:**
//! - Current format!() approach
//! - Optimized itoa approach

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::borrow::Cow;
use std::cell::RefCell;

// ============================================================================
// Fingerprint Benchmarks
// ============================================================================

/// Normalize message - copy of the function from fingerprint.rs for benchmarking
fn normalize_message_current(message: &str) -> Cow<'_, str> {
    // Fast path: check if any transformation is needed
    let needs_digit_replacement = message.bytes().any(|b| b.is_ascii_digit());
    let needs_whitespace_normalization = message
        .bytes()
        .any(|b| b == b'\t' || b == b'\n' || b == b'\r')
        || message.contains("  ")
        || message.starts_with(' ')
        || message.ends_with(' ');
    let needs_message_replacement = message.contains(" message ")
        || message.contains("message #")
        || message.contains(" message#");

    if !needs_digit_replacement && !needs_whitespace_normalization && !needs_message_replacement {
        return Cow::Borrowed(message);
    }

    // Single-pass normalization: handle digits and whitespace together
    let mut normalized = String::with_capacity(message.len());
    let mut chars = message.chars().peekable();
    let mut last_was_whitespace = true;

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            normalized.push('N');
            while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                chars.next();
            }
            last_was_whitespace = false;
        } else if ch.is_whitespace() {
            if !last_was_whitespace {
                normalized.push(' ');
                last_was_whitespace = true;
            }
        } else {
            normalized.push(ch);
            last_was_whitespace = false;
        }
    }

    if normalized.ends_with(' ') {
        normalized.pop();
    }

    // This creates 3 allocations - the issue we want to fix
    if needs_message_replacement {
        let result = normalized
            .replace(" message ", " error ")
            .replace("message #", "error #")
            .replace(" message#", " error#");
        Cow::Owned(result)
    } else {
        Cow::Owned(normalized)
    }
}

/// Optimized normalize_message using Aho-Corasick for multi-pattern replacement
fn normalize_message_optimized(message: &str) -> Cow<'_, str> {
    use aho_corasick::AhoCorasick;
    use once_cell::sync::Lazy;

    // Pre-compiled Aho-Corasick automaton for "message" -> "error" replacements
    static MESSAGE_PATTERNS: Lazy<AhoCorasick> =
        Lazy::new(|| AhoCorasick::new([" message ", "message #", " message#"]).unwrap());
    static MESSAGE_REPLACEMENTS: &[&str] = &[" error ", "error #", " error#"];

    // Fast path: check if any transformation is needed
    let needs_digit_replacement = message.bytes().any(|b| b.is_ascii_digit());
    let needs_whitespace_normalization = message
        .bytes()
        .any(|b| b == b'\t' || b == b'\n' || b == b'\r')
        || message.contains("  ")
        || message.starts_with(' ')
        || message.ends_with(' ');
    let needs_message_replacement = MESSAGE_PATTERNS.find(message).is_some();

    if !needs_digit_replacement && !needs_whitespace_normalization && !needs_message_replacement {
        return Cow::Borrowed(message);
    }

    // Single-pass normalization for digits and whitespace
    let mut normalized = String::with_capacity(message.len());
    let mut chars = message.chars().peekable();
    let mut last_was_whitespace = true;

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            normalized.push('N');
            while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                chars.next();
            }
            last_was_whitespace = false;
        } else if ch.is_whitespace() {
            if !last_was_whitespace {
                normalized.push(' ');
                last_was_whitespace = true;
            }
        } else {
            normalized.push(ch);
            last_was_whitespace = false;
        }
    }

    if normalized.ends_with(' ') {
        normalized.pop();
    }

    // Single-allocation replacement using Aho-Corasick
    if needs_message_replacement {
        let result = MESSAGE_PATTERNS.replace_all(&normalized, MESSAGE_REPLACEMENTS);
        Cow::Owned(result.to_string())
    } else {
        Cow::Owned(normalized)
    }
}

fn bench_fingerprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint");

    // Test messages of varying complexity
    let test_messages = [
        ("short_no_transform", "Connection refused"),
        ("short_with_numbers", "Error #123 on worker 5"),
        ("medium_with_numbers", "Stress test message #2787 from worker 2 at timestamp 1234567890"),
        ("long_with_numbers", "Failed to process request #98765 for user 12345 in transaction 999888777 at line 42 column 15 with error code 500"),
        ("with_whitespace", "  Multiple   spaces   and\ttabs\nand\nnewlines  "),
        ("needs_message_replace", "Test message #123 from worker 2 - additional message info"),
    ];

    for (name, message) in test_messages {
        group.throughput(Throughput::Bytes(message.len() as u64));

        group.bench_with_input(BenchmarkId::new("current", name), &message, |b, msg| {
            b.iter(|| {
                let result = normalize_message_current(black_box(msg));
                black_box(result)
            })
        });

        group.bench_with_input(BenchmarkId::new("optimized", name), &message, |b, msg| {
            b.iter(|| {
                let result = normalize_message_optimized(black_box(msg));
                black_box(result)
            })
        });
    }

    group.finish();
}

// ============================================================================
// JSON Parsing Benchmarks
// ============================================================================

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct SmallPayload {
    id: String,
    level: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MediumPayload {
    id: String,
    project_id: String,
    fingerprint: String,
    level: String,
    message: String,
    exception_type: Option<String>,
    exception_value: Option<String>,
    stacktrace: String,
    context: String,
    tags: String,
    user_data: String,
    service_name: Option<String>,
    timestamp: String,
}

// Thread-local buffer for simd-json
thread_local! {
    static SIMD_JSON_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(65536));
}

fn parse_json_simd<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, simd_json::Error> {
    SIMD_JSON_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.extend_from_slice(bytes);
        simd_json::from_slice(&mut buf)
    })
}

fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    // Create test payloads
    let small_payload = SmallPayload {
        id: "abc123".to_string(),
        level: "error".to_string(),
        message: "Connection refused".to_string(),
    };
    let small_json = serde_json::to_vec(&small_payload).unwrap();

    let medium_payload = MediumPayload {
        id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        project_id: "660e8400-e29b-41d4-a716-446655440000".to_string(),
        fingerprint: "abc123def456".to_string(),
        level: "error".to_string(),
        message: "Connection refused to database server".to_string(),
        exception_type: Some("ConnectionError".to_string()),
        exception_value: Some("Failed to connect".to_string()),
        stacktrace: r#"[{"filename":"app.js","function":"connect","lineno":42}]"#.to_string(),
        context: r#"{"url":"postgres://localhost:5432"}"#.to_string(),
        tags: r#"{"env":"production","version":"1.2.3"}"#.to_string(),
        user_data: r#"{"id":"user123"}"#.to_string(),
        service_name: Some("api-server".to_string()),
        timestamp: "2025-01-15T10:30:00Z".to_string(),
    };
    let medium_json = serde_json::to_vec(&medium_payload).unwrap();

    // Small payload benchmarks
    group.throughput(Throughput::Bytes(small_json.len() as u64));

    group.bench_function("serde_json/small", |b| {
        b.iter(|| {
            let result: SmallPayload = serde_json::from_slice(black_box(&small_json)).unwrap();
            black_box(result)
        })
    });

    group.bench_function("simd_json/small", |b| {
        b.iter(|| {
            let result: SmallPayload = parse_json_simd(black_box(&small_json)).unwrap();
            black_box(result)
        })
    });

    // Medium payload benchmarks
    group.throughput(Throughput::Bytes(medium_json.len() as u64));

    group.bench_function("serde_json/medium", |b| {
        b.iter(|| {
            let result: MediumPayload = serde_json::from_slice(black_box(&medium_json)).unwrap();
            black_box(result)
        })
    });

    group.bench_function("simd_json/medium", |b| {
        b.iter(|| {
            let result: MediumPayload = parse_json_simd(black_box(&medium_json)).unwrap();
            black_box(result)
        })
    });

    group.finish();
}

// ============================================================================
// Rate Limit Key Formatting Benchmarks
// ============================================================================

fn bench_rate_limit_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_limit_keys");

    let user_id = uuid::Uuid::new_v4();
    let user_id_str = user_id.to_string();
    let limit_min: i32 = 240;
    let limit_hour: i32 = 1200;

    // Current approach using format!()
    group.bench_function("format_macro", |b| {
        b.iter(|| {
            let key_min = format!("rate_limit:min:{}:{}", limit_min, user_id_str);
            let key_hour = format!("rate_limit:hour:{}:{}", limit_hour, user_id_str);
            black_box((key_min, key_hour))
        })
    });

    // Optimized approach using itoa and pre-allocated buffer
    group.bench_function("itoa_concat", |b| {
        let mut buffer = itoa::Buffer::new();
        b.iter(|| {
            let min_str = buffer.format(limit_min);
            let key_min = ["rate_limit:min:", min_str, ":", &user_id_str].concat();
            let hour_str = buffer.format(limit_hour);
            let key_hour = ["rate_limit:hour:", hour_str, ":", &user_id_str].concat();
            black_box((key_min, key_hour))
        })
    });

    // Even more optimized: pre-format UUID once and reuse
    group.bench_function("itoa_prealloc", |b| {
        let mut buffer = itoa::Buffer::new();
        let mut key_buffer = String::with_capacity(64);
        b.iter(|| {
            key_buffer.clear();
            key_buffer.push_str("rate_limit:min:");
            key_buffer.push_str(buffer.format(limit_min));
            key_buffer.push(':');
            key_buffer.push_str(&user_id_str);
            let key_min = key_buffer.clone();

            key_buffer.clear();
            key_buffer.push_str("rate_limit:hour:");
            key_buffer.push_str(buffer.format(limit_hour));
            key_buffer.push(':');
            key_buffer.push_str(&user_id_str);
            let key_hour = key_buffer.clone();

            black_box((key_min, key_hour))
        })
    });

    group.finish();
}

// ============================================================================
// Criterion Groups
// ============================================================================

criterion_group!(
    benches,
    bench_fingerprint,
    bench_json_parsing,
    bench_rate_limit_keys,
);

criterion_main!(benches);
