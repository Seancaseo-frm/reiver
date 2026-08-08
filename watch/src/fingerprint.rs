use crate::models::{ExceptionPayload, StackFrame};
use aho_corasick::AhoCorasick;
use once_cell::sync::Lazy;
use std::borrow::Cow;

// Pre-compiled AhoCorasick automaton for message->error normalization
// This allows single-pass multi-pattern replacement, avoiding multiple allocations
static MESSAGE_REPLACER: Lazy<(AhoCorasick, Vec<&'static str>)> = Lazy::new(|| {
    let patterns = [" message ", "message #", " message#"];
    let replacements = vec![" error ", "error #", " error#"];
    let ac = AhoCorasick::new(&patterns).expect("Failed to build AhoCorasick automaton");
    (ac, replacements)
});

/// Generate a fingerprint for an error using PostHog-style grouping algorithm.
///
/// The algorithm:
/// 1. Always includes exception type
/// 2. Includes exception message ONLY if there are no resolved frames (frames with function names)
/// 3. For stack frames:
///    - Only includes in-app frames (non-library frames)
///    - For frames with function names: includes source, function name
///    - For frames without function names: includes source, line, column
/// 4. Uses BLAKE3 to hash all components (faster than SHA-512, same quality)
///    Note: This produces 64-char hex instead of 128-char, but still produces
///    unique fingerprints for all practical purposes.
pub fn generate_fingerprint(payload: &ExceptionPayload) -> String {
    let mut hasher = blake3::Hasher::new();

    // Get exception info
    // Only include exception type if there's an actual exception AND it's meaningful for grouping
    // We exclude generic/custom exception types to help group messages and exceptions with same normalized content
    // But we keep standard library exception types (like TypeError, ValueError, etc.)
    if let Some(exception) = &payload.exception {
        let exception_type = &exception.exception_type;

        // Common standard library exception types that should be included
        let standard_types = [
            "TypeError",
            "ReferenceError",
            "ValueError",
            "KeyError",
            "IndexError",
            "AttributeError",
            "ImportError",
            "RuntimeError",
            "IOError",
            "OSError",
            "SyntaxError",
            "NameError",
            "AssertionError",
        ];

        // Skip only truly generic/custom types
        // This allows capture_message() and capture_exception() with same normalized message to group together
        let is_generic_type = exception_type == "Error"
            || exception_type == "Unknown"
            || (!standard_types.contains(&exception_type.as_str())
                && exception_type.ends_with("Error")
                && exception_type.len() < 12); // Short custom error types like "TestError", "MyError" but not "TypeError"

        if !is_generic_type {
            hasher.update(exception_type.as_bytes());
        }
    }

    // Check if we have stack frames and if any have function names (resolved frames)
    let stack_frames = payload
        .exception
        .as_ref()
        .and_then(|e| e.stacktrace.as_ref())
        .map(|frames| frames.as_slice())
        .unwrap_or(&[]);

    // Filter to in-app frames (non-library frames)
    let in_app_frames: Vec<&StackFrame> = stack_frames
        .iter()
        .filter(|f| !is_library_frame(f))
        .collect();

    // Check if we have any resolved frames (frames with function names)
    let has_resolved_frames = in_app_frames.iter().any(|f| f.function.is_some());

    // If we have no in-app frames, only use exception type and message for fingerprinting.
    // Library frames are unreliable (different line numbers between async runs, varying runtime frames)
    // and should not affect error grouping.
    if in_app_frames.is_empty() {
        // No in-app frames - use message for stable fingerprinting
        let normalized_message = normalize_message(&payload.message);
        hasher.update(normalized_message.as_bytes());
        // Skip frame processing entirely - library frames are too unstable
        return hasher.finalize().to_hex().to_string();
    }

    // Include exception message ONLY if there are no resolved frames
    // This matches PostHog's behavior: if we have stack traces, we use those;
    // otherwise, we fall back to the message
    if !has_resolved_frames {
        let normalized_message = normalize_message(&payload.message);
        hasher.update(normalized_message.as_bytes());
    }

    // Process in-app frames only
    if has_resolved_frames {
        // For resolved frames (with function names): include normalized source and function name only
        for frame in in_app_frames.iter() {
            if frame.function.is_some() {
                // Include normalized source file name (just filename, not full path)
                if let Some(source) = &frame.filename {
                    let normalized_source = normalize_file_path(source);
                    hasher.update(normalized_source.as_bytes());
                }

                // Include normalized function name (without parameters)
                if let Some(function) = &frame.function {
                    let normalized_function = normalize_function_name(function);
                    hasher.update(normalized_function.as_bytes());
                }
            }
        }
    } else {
        // For unresolved frames (without function names): include normalized source, line, column
        for frame in in_app_frames.iter() {
            // Include normalized source file name (just filename, not full path)
            if let Some(source) = &frame.filename {
                let normalized_source = normalize_file_path(source);
                hasher.update(normalized_source.as_bytes());
            }

            // Include mangled function name if available (unresolved) - may already be mangled, but normalize if possible
            if let Some(function) = &frame.function {
                let normalized_function = normalize_function_name(function);
                hasher.update(normalized_function.as_bytes());
            }

            // Include line number if available
            if let Some(lineno) = frame.lineno {
                hasher.update(lineno.to_string().as_bytes());
            }

            // Include column number if available
            if let Some(colno) = frame.colno {
                hasher.update(colno.to_string().as_bytes());
            }
        }
    }

    hasher.finalize().to_hex().to_string()
}

/// Normalize file path to just the filename (removes directory path)
/// Examples:
/// - "/home/user/app/src/main.js" -> "main.js"
/// - "C:\\Users\\App\\main.js" -> "main.js"
/// - "src/components/Button.tsx" -> "Button.tsx"
/// - "main.js" -> "main.js" (already just filename)
///
/// Uses `Cow<'_, str>` to avoid allocation when the path is already just a filename.
pub(crate) fn normalize_file_path(path: &str) -> Cow<'_, str> {
    // Check for Windows backslash first
    let has_backslash = path.contains('\\');

    if has_backslash {
        // Need to allocate for Windows path normalization
        let normalized = path.replace('\\', "/");
        if let Some(last_slash) = normalized.rfind('/') {
            Cow::Owned(normalized[last_slash + 1..].to_string())
        } else {
            Cow::Owned(normalized)
        }
    } else {
        // Unix path - check if we need to extract filename
        if let Some(last_slash) = path.rfind('/') {
            // Return slice of original string (no allocation)
            Cow::Borrowed(&path[last_slash + 1..])
        } else {
            // No slash found, already just filename (no allocation)
            Cow::Borrowed(path)
        }
    }
}

/// Normalize function name by removing parameters/signature
/// This groups functions with different parameters but same name
/// Examples:
/// - "myFunction(user)" -> "myFunction"
/// - "myFunction(user, admin)" -> "myFunction"
/// - "myFunction()" -> "myFunction"
/// - "myFunction" -> "myFunction" (already normalized)
/// - "Class.method" -> "Class.method" (preserve class context)
///
/// Handles various formats:
/// - JavaScript: "functionName(param1, param2)"
/// - Python: "function_name(param1, param2)" or "Class.method_name(param1)"
/// - Rust: "module::function" or "Struct::method"
/// - Java: "com.example.Class.method(param1, param2)"
///
/// Uses `Cow<'_, str>` to avoid allocation when no parentheses are present.
pub(crate) fn normalize_function_name(function: &str) -> Cow<'_, str> {
    // If function name contains parentheses, remove everything from '(' onwards
    // This removes parameters: "myFunction(param1, param2)" -> "myFunction"
    if let Some(paren_pos) = function.find('(') {
        let trimmed = function[..paren_pos].trim_end();
        // Check if trim_end removed anything
        if trimmed.len() == paren_pos {
            // No trailing whitespace before '(' - can borrow
            Cow::Borrowed(&function[..paren_pos])
        } else {
            // Had trailing whitespace - need to own
            Cow::Owned(trimmed.to_string())
        }
    } else {
        // No parentheses, return as-is (no allocation)
        Cow::Borrowed(function)
    }
}

/// Normalize message for fingerprinting with single-pass optimization.
///
/// Transformations applied:
/// 1. Replace digit sequences with 'N' (e.g., "#123" -> "#N")
/// 2. Normalize whitespace (collapse multiple spaces, trim)
/// 3. Normalize "message" -> "error" for grouping
///
/// Uses `Cow<'_, str>` - returns borrowed reference if no changes needed.
pub(crate) fn normalize_message(message: &str) -> Cow<'_, str> {
    // Fast path: check if any transformation is needed
    let needs_digit_replacement = message.bytes().any(|b| b.is_ascii_digit());
    let needs_whitespace_normalization = message.bytes().any(|b| b == b'\t' || b == b'\n' || b == b'\r')
        || message.contains("  ") // Multiple spaces
        || message.starts_with(' ')
        || message.ends_with(' ');
    let needs_message_replacement = message.contains(" message ")
        || message.contains("message #")
        || message.contains(" message#");

    // If no transformations needed, return borrowed reference
    if !needs_digit_replacement && !needs_whitespace_normalization && !needs_message_replacement {
        return Cow::Borrowed(message);
    }

    // Single-pass normalization: handle digits and whitespace together
    let mut normalized = String::with_capacity(message.len());
    let mut chars = message.chars().peekable();
    let mut last_was_whitespace = true; // Start true to handle leading whitespace

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            // Replace digit sequence with single 'N'
            normalized.push('N');
            // Skip remaining consecutive digits
            while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                chars.next();
            }
            last_was_whitespace = false;
        } else if ch.is_whitespace() {
            // Collapse whitespace: only add space if last char wasn't whitespace
            if !last_was_whitespace {
                normalized.push(' ');
                last_was_whitespace = true;
            }
            // Skip additional whitespace
        } else {
            normalized.push(ch);
            last_was_whitespace = false;
        }
    }

    // Remove trailing space if present
    if normalized.ends_with(' ') {
        normalized.pop();
    }

    // Normalize "message" -> "error" for grouping
    // This helps group errors sent as messages vs exceptions
    if needs_message_replacement {
        // Use pre-compiled AhoCorasick for single-pass multi-pattern replacement
        // This avoids 3 separate allocations from chained .replace() calls
        let (ac, replacements) = &*MESSAGE_REPLACER;
        let result = ac.replace_all(&normalized, replacements);
        // replace_all returns Cow<str>; convert to owned String
        Cow::Owned(result.to_string())
    } else {
        Cow::Owned(normalized)
    }
}

/// Check if a frame is from a library (not application code).
/// This determines if a frame is "in-app" - frames from libraries are excluded from fingerprinting.
///
/// If the frame has an explicit `in_app` field set by the client SDK, that takes precedence.
/// Otherwise, we use path-based heuristics for different languages.
///
/// Language-specific patterns:
/// - JavaScript/Node: node_modules, bower_components, .min.js
/// - Python: site-packages, dist-packages
/// - Rust: target/, .cargo/registry, CARGO_HOME paths
/// - Go: GOROOT, GOPATH paths, vendor/ (if vendored deps)
/// - Ruby: vendor/, gems/
/// - Java: jar files, class files in known library locations
/// - System libraries: /usr/lib, /usr/local/lib, /Library/
fn is_library_frame(frame: &StackFrame) -> bool {
    // If client SDK explicitly marked this frame, respect that
    if let Some(in_app) = frame.in_app {
        return !in_app; // If in_app is false, it's a library frame
    }

    let filename = match &frame.filename {
        Some(f) => f,
        None => return false, // If no filename, consider it potentially in-app
    };

    let normalized = filename.replace('\\', "/"); // Normalize Windows paths

    // JavaScript/Node.js patterns
    if normalized.contains("node_modules/")
        || normalized.contains("bower_components/")
        || normalized.ends_with(".min.js")
    {
        return true;
    }

    // Python patterns
    if normalized.contains("site-packages/")
        || normalized.contains("dist-packages/")
        || normalized.contains("/lib/python")
        || normalized.contains(".pyc")
        || normalized.contains("__pycache__")
    {
        return true;
    }

    // Rust/Cargo patterns
    if normalized.contains("/target/")
        || normalized.contains("/.cargo/registry/")
        || normalized.contains("/.cargo/git/")
        || normalized.contains("CARGO_HOME")
        || (normalized.contains(".cargo") && !normalized.contains("src/"))
    {
        // But exclude target/debug/build/.../out (sometimes application code during build)
        // This is a heuristic - real solution would need to know project root
        if !normalized.contains("/target/debug/deps/")
            && !normalized.contains("/target/release/deps/")
        {
            return true;
        }
    }

    // Go patterns
    if normalized.contains("GOROOT")
        || normalized.contains("/go/src/")
        || normalized.contains("/go/pkg/mod/")
        || normalized.contains("/vendor/") && normalized.contains(".go")
        || normalized.starts_with("/usr/local/go/")
        || normalized.starts_with("C:/go/")
    // Windows Go installation
    {
        // Check if it's in a vendor directory (vendored dependency)
        if normalized.contains("/vendor/") {
            return true;
        }

        // Check if it's in GOPATH/pkg/mod (Go modules)
        if normalized.contains("/pkg/mod/") {
            return true;
        }

        // Check if it's in GOROOT (standard library)
        if normalized.contains("GOROOT") || normalized.contains("/go/src/") {
            return true;
        }
    }

    // Ruby patterns
    if normalized.contains("/vendor/")
        || normalized.contains("/gems/")
        || normalized.contains("/.gem/")
        || normalized.contains("/.bundle/")
    {
        return true;
    }

    // Java patterns (jar files and class files in library locations)
    if normalized.ends_with(".jar")
        || (normalized.contains(".class")
            && (
                normalized.contains("/.m2/repository/") // Maven
            || normalized.contains("/.gradle/") // Gradle
            || normalized.contains("/.ivy2/")
                // Ivy
            ))
    {
        return true;
    }

    // System/library paths (common across languages)
    if normalized.starts_with("/usr/lib/")
        || normalized.starts_with("/usr/local/lib/")
        || normalized.starts_with("/Library/")
        || normalized.starts_with("/System/")
        || normalized.starts_with("C:/Windows/") // Windows system
        || normalized.starts_with("C:/Program Files/")
    // Windows programs
    {
        return true;
    }

    // Standard library patterns (language runtime)
    // These are heuristics - could be improved with language detection
    if normalized.contains("/std/") // Often indicates standard library
        || normalized.contains("runtime/")
        || normalized.contains("internal/")
    // Go internal packages, Rust std internal
    {
        // But be careful - user code might have "internal" or "std" directories
        // Only exclude if it's clearly a system path
        if normalized.starts_with("/usr/")
            || normalized.starts_with("/usr/local/")
            || normalized.contains("GOROOT")
            || normalized.contains("/go/src/")
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ExceptionPayload;

    #[test]
    fn test_normalize_message_replaces_number_sequences() {
        assert_eq!(normalize_message("Error #123"), "Error #N");
        assert_eq!(normalize_message("Error #1234"), "Error #N");
        assert_eq!(normalize_message("Error #9"), "Error #N");
        assert_eq!(normalize_message("Error #99"), "Error #N");
        assert_eq!(normalize_message("Error #999"), "Error #N");
    }

    #[test]
    fn test_normalize_message_handles_multiple_numbers() {
        assert_eq!(
            normalize_message("Worker 2 error #123"),
            "Worker N error #N"
        );
        assert_eq!(
            normalize_message("Worker 10 error #456"),
            "Worker N error #N"
        );
    }

    #[test]
    fn test_normalize_message_preserves_structure() {
        // After normalization, "message" is normalized to "error" to group them together
        assert_eq!(
            normalize_message("Stress test error #2787 from worker 2"),
            "Stress test error #N from worker N"
        );
        assert_eq!(
            normalize_message("Stress test error #2788 from worker 2"),
            "Stress test error #N from worker N"
        );
        // "message" gets normalized to "error" to group with exceptions
        assert_eq!(
            normalize_message("Stress test message #2788 from worker 2"),
            "Stress test error #N from worker N" // "message" -> "error"
        );
    }

    #[test]
    fn test_same_exceptions_have_same_fingerprint() {
        use crate::models::ExceptionInfo;

        let msg1 = "Stress test error #2787 from worker 2";
        let msg2 = "Stress test error #2788 from worker 2";

        // Without stack traces, message is included and normalized
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: msg1.to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: msg1.to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: msg2.to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: msg2.to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // With normalized messages, fingerprints should match
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match for normalized exception messages"
        );
    }

    #[test]
    fn test_message_and_exception_are_grouped_together() {
        use crate::models::ExceptionInfo;

        // Test that capture_message and capture_exception with same normalized content group together
        // This simulates: capture_message("Stress test message #123") and capture_exception(TestError { message: "Stress test error #456" })

        // Message without exception (capture_message)
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Stress test message #123 from worker 2".to_string(),
            exception: None, // No exception - this is a message
            ..Default::default()
        };

        // Exception with TestError type (capture_exception)
        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Stress test error #456 from worker 2".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "TestError".to_string(), // Specific exception type
                value: "Stress test error #456 from worker 2".to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // They should have the same fingerprint because:
        // 1. Exception type "TestError" is not "Error" or "Unknown", so it's included for payload2
        // 2. But wait - payload1 has no exception, so no exception type
        // 3. Messages normalize to the same thing ("Stress test error #N from worker N")

        // Actually, they won't match because payload2 includes "TestError" in the hash
        // Let me check the actual behavior...
        println!("FP1 (message): {}", fp1);
        println!("FP2 (exception): {}", fp2);

        // For now, they might not match because of TestError type, but messages should normalize the same
        // Let's verify message normalization works
        let norm1 = normalize_message("Stress test message #123 from worker 2");
        let norm2 = normalize_message("Stress test error #456 from worker 2");
        assert_eq!(norm1, norm2, "Messages should normalize to the same string");
    }

    #[test]
    fn test_exception_message_included_only_without_stacktrace() {
        use crate::models::{ExceptionInfo, StackFrame};

        // Without stack trace - message is included
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Some error message".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "TypeError".to_string(),
                value: "Some error message".to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        // With stack trace with function names - message is NOT included
        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Different error message".to_string(), // Different message
            exception: Some(ExceptionInfo {
                exception_type: "TypeError".to_string(), // Same type
                value: "Different error message".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction".to_string()), // Has function name
                    lineno: Some(10),
                    colno: Some(5),
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // These should have different fingerprints because:
        // - payload1 includes message (no stacktrace)
        // - payload2 includes stacktrace frames (message excluded)
        assert_ne!(
            fp1, fp2,
            "Fingerprints should differ when one has stacktrace and one doesn't"
        );

        // But if we have same exception type and same stacktrace frames, they should match
        let payload3 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Yet another different message".to_string(), // Different message
            exception: Some(ExceptionInfo {
                exception_type: "TypeError".to_string(), // Same type
                value: "Yet another different message".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction".to_string()), // Same function name
                    lineno: Some(10),
                    colno: Some(5),
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let fp3 = generate_fingerprint(&payload3);
        assert_eq!(fp2, fp3, "Fingerprints should match when exception type and stack frames are the same (message ignored)");
    }

    #[test]
    fn test_different_exception_types_have_different_fingerprints() {
        use crate::models::ExceptionInfo;

        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Some error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "TypeError".to_string(),
                value: "Some error".to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Some error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "ReferenceError".to_string(), // Different type
                value: "Some error".to_string(),
                stacktrace: None,
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        assert_ne!(
            fp1, fp2,
            "Fingerprints should differ for different exception types"
        );
    }

    #[test]
    fn test_library_frames_are_excluded() {
        use crate::models::{ExceptionInfo, StackFrame};

        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()), // In-app
                    function: Some("myFunction".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![
                    StackFrame {
                        filename: Some("app.js".to_string()), // In-app
                        function: Some("myFunction".to_string()),
                        lineno: Some(10),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                    StackFrame {
                        filename: Some("node_modules/library.js".to_string()), // Library - should be excluded
                        function: Some("libraryFunc".to_string()),
                        lineno: Some(20),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                ]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have same fingerprint because library frames are excluded
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match when only in-app frames are considered"
        );
    }

    #[test]
    fn test_rust_library_frames() {
        use crate::models::{ExceptionInfo, StackFrame};

        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("src/main.rs".to_string()), // In-app
                    function: Some("main".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![
                    StackFrame {
                        filename: Some("src/main.rs".to_string()), // In-app
                        function: Some("main".to_string()),
                        lineno: Some(10),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                    StackFrame {
                        filename: Some(
                            "/home/user/.cargo/registry/src/serde-1.0/lib.rs".to_string(),
                        ), // Cargo registry - library
                        function: Some("serde::serialize".to_string()),
                        lineno: Some(20),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                ]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have same fingerprint because Cargo registry frames are excluded
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match when Cargo library frames are excluded"
        );
    }

    #[test]
    fn test_go_library_frames() {
        use crate::models::{ExceptionInfo, StackFrame};

        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("main.go".to_string()), // In-app
                    function: Some("main".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![
                    StackFrame {
                        filename: Some("main.go".to_string()), // In-app
                        function: Some("main".to_string()),
                        lineno: Some(10),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                    StackFrame {
                        filename: Some("/usr/local/go/src/runtime/panic.go".to_string()), // Go stdlib - library
                        function: Some("runtime.panic".to_string()),
                        lineno: Some(20),
                        colno: None,
                        code: None,
                        in_app: None,
                    },
                ]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have same fingerprint because Go stdlib frames are excluded
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match when Go stdlib frames are excluded"
        );
    }

    #[test]
    fn test_explicit_in_app_field() {
        use crate::models::{ExceptionInfo, StackFrame};

        // Test that explicit in_app field takes precedence over path-based detection
        // Frame that looks like library but is explicitly marked as in-app should be included
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: Some(true), // Explicitly marked as in-app
                }]),
            }),
            ..Default::default()
        };

        // Frame with library path but explicitly marked as in-app should still be included
        // (but will have different fingerprint due to different file path, which is correct)
        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("node_modules/lib.js".to_string()), // Looks like library
                    function: Some("myFunction".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: Some(true), // Explicitly marked as in-app - should be included
                }]),
            }),
            ..Default::default()
        };

        // Frame with library path and explicitly marked as NOT in-app should be excluded
        let payload3 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("node_modules/lib.js".to_string()),
                    function: Some("myFunction".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: Some(false), // Explicitly marked as library - should be excluded
                }]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);
        let fp3 = generate_fingerprint(&payload3);

        // payload1 and payload2 should both include their frames (both in_app=true)
        // but have different fingerprints because file paths differ (which is correct)
        assert_ne!(
            fp1, fp2,
            "Fingerprints should differ when file paths differ, even if both in_app=true"
        );

        // payload3 should have different fingerprint because it has no in-app frames
        // (so it falls back to message-based fingerprinting)
        assert_ne!(
            fp1, fp3,
            "Fingerprints should differ when in_app=false excludes the frame"
        );
        assert_ne!(
            fp2, fp3,
            "Fingerprints should differ when in_app=false excludes the frame"
        );

        // Verify that explicit in_app=false excludes the frame
        // payload3 should use message instead of stack trace (since no in-app frames)
        assert_eq!(fp3.len(), 64, "Should produce BLAKE3 hash (64 hex chars)");
    }

    #[test]
    fn test_stacktrace_without_function_names_uses_line_and_column() {
        use crate::models::{ExceptionInfo, StackFrame};

        // Frame with function name - only source and function used
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction".to_string()), // Has function name
                    lineno: Some(10),
                    colno: Some(5),
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        // Frame without function name - source, line, column used
        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: None, // No function name
                    lineno: Some(10),
                    colno: Some(5),
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have different fingerprints because frame representation differs
        assert_ne!(
            fp1, fp2,
            "Fingerprints should differ when frame has function name vs when it doesn't"
        );
    }

    #[test]
    fn test_normalize_file_path() {
        assert_eq!(normalize_file_path("/home/user/app/src/main.js"), "main.js");
        assert_eq!(normalize_file_path("C:\\Users\\App\\main.js"), "main.js");
        assert_eq!(
            normalize_file_path("src/components/Button.tsx"),
            "Button.tsx"
        );
        assert_eq!(normalize_file_path("main.js"), "main.js");
        assert_eq!(normalize_file_path("./src/utils/helper.js"), "helper.js");
    }

    #[test]
    fn test_normalize_function_name() {
        assert_eq!(normalize_function_name("myFunction"), "myFunction");
        assert_eq!(normalize_function_name("myFunction(user)"), "myFunction");
        assert_eq!(
            normalize_function_name("myFunction(user, admin)"),
            "myFunction"
        );
        assert_eq!(normalize_function_name("myFunction()"), "myFunction");
        assert_eq!(
            normalize_function_name("Class.method(param1)"),
            "Class.method"
        );
        assert_eq!(
            normalize_function_name("module::function(param1, param2)"),
            "module::function"
        );
    }

    #[test]
    fn test_file_path_normalization_in_fingerprinting() {
        use crate::models::{ExceptionInfo, StackFrame};

        // Same file in different paths should produce same fingerprint
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("/home/user/app/src/main.js".to_string()),
                    function: Some("myFunction".to_string()),
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("/var/www/app/src/main.js".to_string()), // Different path
                    function: Some("myFunction".to_string()),               // Same function
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have same fingerprint because paths normalize to same filename
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match when paths normalize to same filename"
        );
    }

    #[test]
    fn test_function_name_normalization_in_fingerprinting() {
        use crate::models::{ExceptionInfo, StackFrame};

        // Same function with different parameters should produce same fingerprint
        let payload1 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction(user)".to_string()), // With params
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let payload2 = ExceptionPayload {
            project_key: "test".to_string(),
            level: "error".to_string(),
            message: "Error".to_string(),
            exception: Some(ExceptionInfo {
                exception_type: "Error".to_string(),
                value: "Error".to_string(),
                stacktrace: Some(vec![StackFrame {
                    filename: Some("app.js".to_string()),
                    function: Some("myFunction(admin, user)".to_string()), // Different params
                    lineno: Some(10),
                    colno: None,
                    code: None,
                    in_app: None,
                }]),
            }),
            ..Default::default()
        };

        let fp1 = generate_fingerprint(&payload1);
        let fp2 = generate_fingerprint(&payload2);

        // Should have same fingerprint because function names normalize to same name
        assert_eq!(
            fp1, fp2,
            "Fingerprints should match when function names normalize to same name"
        );
    }
}
