use crate::transport::{ErrorPayload, ExceptionPayload};
use backtrace::Backtrace;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::Ordering;

/// Stack frame information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackFrame {
    pub filename: Option<String>,
    pub function: Option<String>,
    pub lineno: Option<u32>,
    pub colno: Option<u32>,
    pub code: Option<String>,
    /// Whether this frame is from application code (true) or library code (false).
    #[serde(default)]
    pub in_app: Option<bool>,
}

/// Exception information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exception {
    #[serde(rename = "type")]
    pub exception_type: String,
    pub value: String,
    pub stacktrace: Option<Vec<StackFrame>>,
}

/// Get trace_id and span_id from OpenTelemetry context (if available).
///
/// Returns `(trace_id, span_id)` when the `opentelemetry` feature is enabled
/// and there is an active valid span context. Otherwise returns `(None, None)`.
pub(crate) fn get_trace_context() -> (Option<String>, Option<String>) {
    #[cfg(feature = "opentelemetry")]
    {
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry::Context;

        let context = Context::current();
        let span = context.span();
        let span_context = span.span_context();
        if span_context.is_valid() {
            // TraceId and SpanId implement Display which outputs hex format
            let trace_id = Some(format!("{}", span_context.trace_id()));
            let span_id = Some(format!("{}", span_context.span_id()));
            (trace_id, span_id)
        } else {
            (None, None)
        }
    }
    #[cfg(not(feature = "opentelemetry"))]
    {
        (None, None)
    }
}

/// Build an ErrorPayload (helper function)
fn build_payload(
    options: &crate::ClientOptions,
    message: String,
    level: &str,
    exception: Option<ExceptionPayload>,
    context: Option<serde_json::Value>,
    tags: Option<serde_json::Value>,
    user: Option<serde_json::Value>,
) -> ErrorPayload {
    let mut context_value = context.unwrap_or_else(|| json!({}));

    // Add runtime context
    if let Some(obj) = context_value.as_object_mut() {
        obj.insert(
            "runtime".to_string(),
            json!({
                "name": "rust",
            }),
        );

        if let Some(env) = &options.environment {
            obj.insert("environment".to_string(), json!(env));
        }
    }

    // Merge tags
    let mut tags_value = options
        .tags
        .clone()
        .map(|t| {
            let mut map = json!({});
            for (k, v) in t {
                map[k] = json!(v);
            }
            map
        })
        .unwrap_or_else(|| json!({}));

    if let Some(custom_tags) = tags {
        if let (Some(obj1), Some(obj2)) = (tags_value.as_object_mut(), custom_tags.as_object()) {
            for (k, v) in obj2 {
                obj1.insert(k.clone(), v.clone());
            }
        }
    }

    // Get trace context from OpenTelemetry (trace_id and span_id)
    let (trace_id, span_id) = get_trace_context();

    ErrorPayload {
        api_key: options.api_key.clone().expect("api_key must be set"),
        timestamp: Some(chrono::Utc::now()),
        level: level.to_string(),
        message,
        exception,
        context: Some(context_value),
        tags: Some(tags_value),
        user,
        trace_id,
        span_id,
        service_name: options.service_name.clone(),
        environment: options.environment.clone(),
        version: options.version.clone(),
        deployment_id: options.deployment_id.clone(),
        repository_url: options.repository_url.clone(),
        region: options.region.clone(),
        host_name: options.host_name.clone(),
        runtime: Some("rust".to_string()),
        pod_name: options.pod_name.clone(),
        cluster_name: options.cluster_name.clone(),
        container_id: options.container_id.clone(),
        http_method: None,
        http_url: None,
        user_id: None,
    }
}

/// Parse the type name from `Debug` output.
///
/// This is based on Sentry's approach - parse the first word from Debug output
/// which typically contains the type name.
fn parse_type_from_debug(d: &str) -> &str {
    d.split(&[' ', '(', '{', '\r', '\n'][..])
        .next()
        .unwrap_or("Error")
        .trim()
}

/// Extract exception type and value from an error (based on Sentry's approach).
///
/// For most errors, this parses the type name from the Debug output.
/// For anyhow errors (where Debug == Display), it returns "Error".
fn exception_from_error<E: std::error::Error + ?Sized>(err: &E) -> (String, String) {
    let dbg = format!("{err:?}");
    let value = err.to_string();

    // A generic `anyhow::Error` will just `Debug::fmt` the `String` that you feed
    // it. Trying to parse the type name from that will result in a leading quote
    // and the first word, so quite useless.
    // To work around this, we check if the `Debug::fmt` of the complete error
    // matches its `Display::fmt`, in which case there is no type to parse and
    // we will just be using `Error`.
    let ty = if dbg == format!("{value:?}") {
        String::from("Error")
    } else {
        parse_type_from_debug(&dbg).to_owned()
    };

    (ty, value)
}

/// Capture an exception using the global client (drop-in replacement for Sentry::capture_exception)
pub fn capture_exception(error: &dyn std::error::Error) {
    if let Some((sender, options, pending_count)) = crate::get_global_state() {
        let (exception_type, error_value) = exception_from_error(error);

        // Capture backtrace using the backtrace crate (like Sentry does)
        // This works even without RUST_BACKTRACE=1, though the quality depends on debug symbols
        let backtrace = Backtrace::new();
        let stacktrace = {
            let frames = extract_stack_frames(&backtrace);
            if !frames.is_empty() {
                Some(
                    frames
                        .into_iter()
                        .map(|f| stack_frame_to_payload(&f))
                        .collect(),
                )
            } else {
                None
            }
        };

        let exception_payload = ExceptionPayload {
            exception_type: exception_type.clone(),
            value: error_value.clone(),
            stacktrace,
        };

        let payload = build_payload(
            &options,
            error_value,
            "error",
            Some(exception_payload),
            None,
            None,
            None,
        );

        pending_count.fetch_add(1, Ordering::Relaxed);
        let _ = sender.send(payload);
    } else {
        eprintln!("Reiver: Client not initialized. Call reiver::init() first.");
    }
}

/// Capture a message using the global client (drop-in replacement for Sentry::capture_message)
pub fn capture_message(message: &str, level: &str) {
    if let Some((sender, options, pending_count)) = crate::get_global_state() {
        let payload = build_payload(&options, message.to_string(), level, None, None, None, None);

        pending_count.fetch_add(1, Ordering::Relaxed);
        let _ = sender.send(payload);
    } else {
        eprintln!("Reiver: Client not initialized. Call reiver::init() first.");
    }
}

/// Extract stack frames from a backtrace
/// Sentry-style: uses the backtrace crate for structured access to frames and symbols
pub fn extract_stack_frames(backtrace: &Backtrace) -> Vec<StackFrame> {
    let mut frames = Vec::new();

    // Iterate through frames (like Sentry does)
    for frame in backtrace.frames() {
        // For each frame, there may be multiple symbols if a function was inlined
        let symbols = frame.symbols();

        for symbol in symbols {
            let abs_path = symbol.filename().map(|p| p.to_string_lossy().to_string());
            let filename = abs_path
                .as_ref()
                .map(|p| {
                    // Extract just the filename from the path
                    p.rsplit(&['/', '\\'][..]).next().unwrap_or(p)
                })
                .map(String::from);

            let function = symbol
                .name()
                .map(|n| {
                    // Demangle Rust symbols - basic demangling
                    let name_str = n.to_string();
                    // Rust symbols are already somewhat readable, but we can clean them up
                    name_str
                })
                .or_else(|| Some("<unknown>".to_string()));

            // Determine if this frame is in-app (user code) or library/stdlib code
            let in_app = abs_path.as_ref().map(|path| {
                let normalized = path.replace('\\', "/");
                // Rust standard library
                if normalized.contains("/rustc/")
                    || normalized.contains("/rust/src/")
                    || normalized.contains("/std/")
                    || normalized.starts_with("/rust/")
                {
                    return false;
                }
                // Cargo registry (external crates)
                if normalized.contains("/.cargo/registry/")
                    || normalized.contains("/.cargo/git/")
                    || (normalized.contains(".cargo") && !normalized.contains("src/"))
                {
                    return false;
                }
                // Rustup toolchain
                if normalized.contains("/.rustup/") {
                    return false;
                }
                // Target directory (built artifacts, but might also have user code)
                // We'll be conservative and mark target/debug/deps as library, but allow target/src as in-app
                if normalized.contains("/target/debug/deps/")
                    || normalized.contains("/target/release/deps/")
                    || normalized.contains("/target/.rustc_info.json")
                {
                    return false;
                }
                // System libraries
                if normalized.starts_with("/usr/lib/")
                    || normalized.starts_with("/usr/local/lib/")
                    || normalized.starts_with("/Library/")
                    || normalized.starts_with("/System/")
                    || normalized.starts_with("C:/Windows/")
                    || normalized.starts_with("C:/Program Files/")
                {
                    return false;
                }
                // Otherwise, consider it in-app (user code)
                true
            });

            frames.push(StackFrame {
                filename: filename.or_else(|| abs_path.clone()),
                function,
                lineno: symbol.lineno().map(|n| n as u32),
                colno: symbol.colno().map(|n| n as u32),
                code: None,
                in_app,
            });
        }

        // If there were no symbols at all, add at least one frame with the instruction pointer
        if symbols.is_empty() {
            frames.push(StackFrame {
                filename: None,
                function: Some("<unknown>".to_string()),
                lineno: None,
                colno: None,
                code: None,
                in_app: None, // Can't determine without filename
            });
        }
    }

    // Reverse frames so they're in call order (oldest to newest)
    // The backtrace crate returns frames from newest to oldest
    frames.reverse();

    frames
}

/// Convert StackFrame to StackFramePayload (for transport)
pub fn stack_frame_to_payload(frame: &StackFrame) -> crate::transport::StackFramePayload {
    crate::transport::StackFramePayload {
        filename: frame.filename.clone(),
        function: frame.function.clone(),
        lineno: frame.lineno.map(|n| n as i32),
        colno: frame.colno.map(|n| n as i32),
        code: frame.code.clone(),
        in_app: frame.in_app,
    }
}
