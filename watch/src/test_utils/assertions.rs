//! Custom test assertions
//!
//! Provides domain-specific assertions for common testing patterns.

use serde_json::Value;

// ============================================================================
// JSON Assertions
// ============================================================================

/// Assert that a JSON value contains a specific key
#[macro_export]
macro_rules! assert_json_has_key {
    ($json:expr, $key:expr) => {
        assert!(
            $json.get($key).is_some(),
            "Expected JSON to have key '{}', but it was not found.\nJSON: {:?}",
            $key,
            $json
        );
    };
}

/// Assert that a JSON value has a specific key with a specific value
#[macro_export]
macro_rules! assert_json_eq {
    ($json:expr, $key:expr, $expected:expr) => {
        let actual = $json.get($key);
        assert!(
            actual.is_some(),
            "Expected JSON to have key '{}', but it was not found.\nJSON: {:?}",
            $key,
            $json
        );
        assert_eq!(
            actual.unwrap(),
            &serde_json::json!($expected),
            "Key '{}' did not match expected value",
            $key
        );
    };
}

/// Assert that a JSON array has a specific length
#[macro_export]
macro_rules! assert_json_array_len {
    ($json:expr, $expected_len:expr) => {
        let arr = $json.as_array();
        assert!(
            arr.is_some(),
            "Expected JSON to be an array, but got: {:?}",
            $json
        );
        assert_eq!(
            arr.unwrap().len(),
            $expected_len,
            "Expected array length {}, but got {}",
            $expected_len,
            arr.unwrap().len()
        );
    };
}

// ============================================================================
// Response Assertions
// ============================================================================

/// Assert that an HTTP response has a specific status code
pub fn assert_status(response: &axum::http::Response<axum::body::Body>, expected: u16) {
    assert_eq!(
        response.status().as_u16(),
        expected,
        "Expected status {}, got {}",
        expected,
        response.status()
    );
}

/// Assert that an HTTP response has a specific header
pub fn assert_header(
    response: &axum::http::Response<axum::body::Body>,
    name: &str,
    expected: &str,
) {
    let header = response.headers().get(name);
    assert!(
        header.is_some(),
        "Expected header '{}' to be present, but it was not",
        name
    );
    assert_eq!(
        header.unwrap().to_str().unwrap(),
        expected,
        "Header '{}' did not match expected value",
        name
    );
}

/// Assert that an HTTP response has a header present (any value)
pub fn assert_has_header(response: &axum::http::Response<axum::body::Body>, name: &str) {
    assert!(
        response.headers().get(name).is_some(),
        "Expected header '{}' to be present, but it was not",
        name
    );
}

// ============================================================================
// Error Assertions
// ============================================================================

/// Assert that a Result is an error with a specific message substring
#[macro_export]
macro_rules! assert_error_contains {
    ($result:expr, $substring:expr) => {
        let err = $result.expect_err("Expected an error, but got Ok");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains($substring),
            "Expected error to contain '{}', but got: {}",
            $substring,
            err_msg
        );
    };
}

/// Assert that a Result is Ok
#[macro_export]
macro_rules! assert_ok {
    ($result:expr) => {
        assert!(
            $result.is_ok(),
            "Expected Ok, but got Err: {:?}",
            $result.err()
        );
    };
}

/// Assert that a Result is Err
#[macro_export]
macro_rules! assert_err {
    ($result:expr) => {
        assert!(
            $result.is_err(),
            "Expected Err, but got Ok: {:?}",
            $result.ok()
        );
    };
}

// ============================================================================
// UUID Assertions
// ============================================================================

/// Assert that a string is a valid UUID
pub fn assert_valid_uuid(value: &str) {
    assert!(
        uuid::Uuid::parse_str(value).is_ok(),
        "Expected a valid UUID, but got: {}",
        value
    );
}

/// Assert that a JSON value contains a valid UUID at a specific key
pub fn assert_json_uuid(json: &Value, key: &str) {
    let value = json.get(key);
    assert!(value.is_some(), "Expected key '{}' to be present", key);

    let str_value = value.unwrap().as_str();
    assert!(str_value.is_some(), "Expected '{}' to be a string", key);

    assert_valid_uuid(str_value.unwrap());
}

// ============================================================================
// Timing Assertions
// ============================================================================

/// Assert that an operation completes within a time limit
#[macro_export]
macro_rules! assert_completes_within {
    ($duration:expr, $block:block) => {{
        let start = std::time::Instant::now();
        let result = $block;
        let elapsed = start.elapsed();
        assert!(
            elapsed <= $duration,
            "Expected operation to complete within {:?}, but it took {:?}",
            $duration,
            elapsed
        );
        result
    }};
}

// ============================================================================
// Collection Assertions
// ============================================================================

/// Assert that a vector contains an element matching a predicate
#[macro_export]
macro_rules! assert_contains {
    ($vec:expr, $predicate:expr) => {
        assert!(
            $vec.iter().any($predicate),
            "Expected collection to contain an element matching the predicate, but none found.\nCollection: {:?}",
            $vec
        );
    };
}

/// Assert that a vector does not contain any element matching a predicate
#[macro_export]
macro_rules! assert_not_contains {
    ($vec:expr, $predicate:expr) => {
        assert!(
            !$vec.iter().any($predicate),
            "Expected collection to NOT contain an element matching the predicate, but found one.\nCollection: {:?}",
            $vec
        );
    };
}

// ============================================================================
// String Assertions
// ============================================================================

/// Assert that a string matches a regex pattern
pub fn assert_matches_regex(value: &str, pattern: &str) {
    let re = regex::Regex::new(pattern).expect("Invalid regex pattern");
    assert!(
        re.is_match(value),
        "Expected '{}' to match pattern '{}', but it didn't",
        value,
        pattern
    );
}

/// Assert that a string is a valid email format
pub fn assert_valid_email(email: &str) {
    assert!(
        email.contains('@') && email.contains('.'),
        "Expected a valid email format, but got: {}",
        email
    );
}

/// Assert that a string is not empty and not just whitespace
pub fn assert_not_blank(value: &str) {
    assert!(
        !value.trim().is_empty(),
        "Expected non-blank string, but got: '{}'",
        value
    );
}

// ============================================================================
// Numeric Assertions
// ============================================================================

/// Assert that a value is approximately equal to another (within epsilon)
pub fn assert_approx_eq(actual: f64, expected: f64, epsilon: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff <= epsilon,
        "Expected {} to be approximately equal to {} (within {}), but difference was {}",
        actual,
        expected,
        epsilon,
        diff
    );
}

/// Assert that a value is within a range
pub fn assert_in_range<T: PartialOrd + std::fmt::Debug>(value: T, min: T, max: T) {
    assert!(
        value >= min && value <= max,
        "Expected {:?} to be in range [{:?}, {:?}]",
        value,
        min,
        max
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_assert_json_has_key() {
        let json = json!({"name": "test", "value": 42});
        assert_json_has_key!(json, "name");
        assert_json_has_key!(json, "value");
    }

    #[test]
    fn test_assert_json_eq() {
        let json = json!({"name": "test", "value": 42});
        assert_json_eq!(json, "name", "test");
        assert_json_eq!(json, "value", 42);
    }

    #[test]
    fn test_assert_json_array_len() {
        let json = json!([1, 2, 3]);
        assert_json_array_len!(json, 3);
    }

    #[test]
    fn test_assert_valid_uuid() {
        assert_valid_uuid("550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_assert_approx_eq() {
        assert_approx_eq(3.14159, 3.14, 0.01);
    }

    #[test]
    fn test_assert_in_range() {
        assert_in_range(5, 1, 10);
        assert_in_range(1.5, 1.0, 2.0);
    }

    #[test]
    fn test_assert_valid_email() {
        assert_valid_email("test@example.com");
    }

    #[test]
    fn test_assert_not_blank() {
        assert_not_blank("hello");
        assert_not_blank("  hello  ");
    }
}
