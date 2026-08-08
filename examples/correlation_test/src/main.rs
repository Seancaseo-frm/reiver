//! Reiver Correlation Test
//!
//! This integration test creates correlated traces, spans, logs, and errors
//! to test the trace-log-error correlation feature in the Reiver UI.
//!
//! Usage:
//!   cargo run -- [options]
//!
//! What it does:
//! 1. Creates a trace with multiple spans (simulating a request flow)
//! 2. Sends logs via OTLP with automatic trace_id/span_id correlation
//! 3. Uses the Reiver SDK to capture an error with trace_id correlation
//! 4. All three (trace, logs, error) share the same trace_id for correlation

use anyhow::Result;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use rand::Rng;
use std::time::Duration;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// Custom error type for testing - implements std::error::Error
/// so it can be captured by the Reiver SDK
#[derive(Debug, thiserror::Error)]
#[error("PaymentError: {message}")]
struct PaymentError {
    message: String,
}

impl PaymentError {
    fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

/// Configuration for the test
#[derive(Debug, Clone)]
struct TestConfig {
    /// API Key - used for authentication (from project_keys table)
    api_key: String,
    api_url: String,
    service_name: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            // Default values from generate_realistic_data.py
            //project_id: "afd5b451-ba09-41f5-9801-b6112b814080".to_string(),
            api_key: "SbmmoIQZk02Py8Oik7N2fobrstnvtOq6".to_string(),
            api_url: "http://localhost:3000".to_string(),
            service_name: "correlation-test-service".to_string(),
        }
    }
}

/// Initialize OpenTelemetry tracing with OTLP exporter (v0.31 API)
fn init_tracer(config: &TestConfig) -> Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_otlp::{SpanExporter, Protocol};
    
    // Reiver OTLP endpoint is at /api/v1/traces
    let otlp_endpoint = format!("{}/api/v1/traces", config.api_url);
    println!("   OTLP traces endpoint: {}", otlp_endpoint);
    
    // Build HTTP client with custom headers
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-api-key", config.api_key.parse().unwrap());
            headers
        })
        .build()?;
    
    // Build the OTLP span exporter with JSON protocol
    let exporter = SpanExporter::builder()
        .with_http()
        .with_http_client(http_client.clone())
        .with_endpoint(&otlp_endpoint)
        .with_protocol(Protocol::HttpJson)
        .build()?;
    
    // Build the tracer provider with SIMPLE exporter (synchronous, no threading issues)
    // Resource attributes are included in spans and extracted by the backend when linking exceptions
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .with_resource(Resource::builder()
            .with_service_name(config.service_name.clone())
            // All supported resource attributes for backend enrichment
            .with_attribute(KeyValue::new("service.version", "2.4.1"))
            .with_attribute(KeyValue::new("deployment.environment", "production"))
            .with_attribute(KeyValue::new("deployment.id", "deploy-20250122-abc123"))
            .with_attribute(KeyValue::new("cloud.region", "us-east-1"))
            .with_attribute(KeyValue::new("host.name", "payment-worker-7b8f9d6c4-xk2mn"))
            .with_attribute(KeyValue::new("process.runtime.description", "rust 1.75.0"))
            .with_attribute(KeyValue::new("k8s.pod.name", "correlation-test-pod-abc123"))
            .with_attribute(KeyValue::new("k8s.cluster.name", "prod-cluster-east"))
            .with_attribute(KeyValue::new("container.id", "docker://a1b2c3d4e5f6g7h8"))
            .build())
        .build();
    
    // Set as global tracer provider
    opentelemetry::global::set_tracer_provider(provider.clone());

    Ok(provider)
}

/// Initialize OpenTelemetry logging with OTLP exporter (v0.31 API)
fn init_logs(config: &TestConfig) -> Result<opentelemetry_sdk::logs::SdkLoggerProvider> {
    use opentelemetry_sdk::logs::SdkLoggerProvider;
    use opentelemetry_otlp::{LogExporter, Protocol};
    
    // Reiver OTLP endpoint is at /api/v1/logs
    let otlp_endpoint = format!("{}/api/v1/logs", config.api_url);
    
    // Build HTTP client with custom headers
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-api-key", config.api_key.parse().unwrap());
            headers
        })
        .build()?;
    
    // Build the OTLP log exporter with JSON protocol
    let exporter = LogExporter::builder()
        .with_http()
        .with_http_client(http_client)
        .with_endpoint(&otlp_endpoint)
        .with_protocol(Protocol::HttpJson)
        .build()?;
    
    // Build the logger provider with SIMPLE exporter (synchronous, no threading issues)
    // Same resource attributes as tracer for consistency
    let provider = SdkLoggerProvider::builder()
        .with_simple_exporter(exporter)
        .with_resource(Resource::builder()
            .with_service_name(config.service_name.clone())
            .with_attribute(KeyValue::new("service.version", "2.4.1"))
            .with_attribute(KeyValue::new("deployment.environment", "production"))
            .with_attribute(KeyValue::new("deployment.id", "deploy-20250122-abc123"))
            .with_attribute(KeyValue::new("cloud.region", "us-east-1"))
            .with_attribute(KeyValue::new("host.name", "payment-worker-7b8f9d6c4-xk2mn"))
            .with_attribute(KeyValue::new("process.runtime.description", "rust 1.75.0"))
            .with_attribute(KeyValue::new("k8s.pod.name", "correlation-test-pod-abc123"))
            .with_attribute(KeyValue::new("k8s.cluster.name", "prod-cluster-east"))
            .with_attribute(KeyValue::new("container.id", "docker://a1b2c3d4e5f6g7h8"))
            .build())
        .build();

    Ok(provider)
}

/// Get the current trace_id and span_id from OpenTelemetry context
fn get_current_trace_context() -> (Option<String>, Option<String>) {
    let context = opentelemetry::Context::current();
    let span = context.span();
    let span_context = span.span_context();
    
    if span_context.is_valid() {
        let trace_id = format!("{}", span_context.trace_id());
        let span_id = format!("{}", span_context.span_id());
        (Some(trace_id), Some(span_id))
    } else {
        (None, None)
    }
}

/// Simulate a database operation within a span
async fn simulate_db_query() -> Result<()> {
    use opentelemetry::trace::Tracer;
    let tracer = opentelemetry::global::tracer("correlation-test");
    
    let span = tracer
        .span_builder("SELECT FROM orders")
        .with_kind(opentelemetry::trace::SpanKind::Client)
        .with_attributes(vec![
            KeyValue::new("db.system", "postgresql"),
            KeyValue::new("db.statement", "SELECT * FROM orders WHERE user_id = $1"),
            KeyValue::new("db.name", "reiver_test"),
        ])
        .start(&tracer);
    
    let cx = opentelemetry::Context::current_with_span(span);
    let _guard = cx.attach();
    
    // Simulate DB latency
    tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50))).await;
    
    // Send a correlated log via OTLP
    tracing::info!("Executed database query: SELECT FROM orders funny");
    
    Ok(())
}

/// Simulate an external HTTP call within a span
async fn simulate_http_call(target: &str) -> Result<()> {
    use opentelemetry::trace::Tracer;
    let tracer = opentelemetry::global::tracer("correlation-test");
    
    let span = tracer
        .span_builder(format!("HTTP GET {}", target))
        .with_kind(opentelemetry::trace::SpanKind::Client)
        .with_attributes(vec![
            KeyValue::new("http.method", "GET"),
            KeyValue::new("http.url", format!("http://{}/api/v1/data", target)),
            KeyValue::new("peer.service", target.to_string()),
        ])
        .start(&tracer);
    
    let cx = opentelemetry::Context::current_with_span(span);
    let _guard = cx.attach();
    
    // Simulate HTTP latency
    tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(50..150))).await;
    
    // Send a correlated log via OTLP
    tracing::info!(target_service = target, "Called external service funny");
    
    Ok(())
}

/// Main test scenario: simulate a request that goes through multiple services
async fn run_test_scenario(
    config: &TestConfig,
    _reiver_guard: &reiver_sdk::Guard,
) -> Result<()> {
    use opentelemetry::trace::Tracer;
    let tracer = opentelemetry::global::tracer("correlation-test");
    
    println!("\n🚀 Starting correlation test scenario...\n");
    
    // Create the root span (entry point)
    let root_span = tracer
        .span_builder("POST /api/orders")
        .with_kind(opentelemetry::trace::SpanKind::Server)
        .with_attributes(vec![
            KeyValue::new("http.method", "POST"),
            KeyValue::new("http.url", "/api/orders"),
            KeyValue::new("http.status_code", 500i64),
        ])
        .start(&tracer);
    
    let root_cx = opentelemetry::Context::current_with_span(root_span);
    let _root_guard = root_cx.attach();
    
    let (trace_id, span_id) = get_current_trace_context();
    println!("📍 Created trace:");
    println!("   trace_id: {}", trace_id.clone().unwrap_or_default());
    println!("   root_span_id: {}", span_id.unwrap_or_default());
    
    // Step 1: Log the incoming request (via OTLP)
    tracing::info!(endpoint = "/api/orders", method = "POST", "Received order request funny");
    println!("   ✓ Sent log: Received POST /api/orders request");
    
    // Step 2: Validate input
    {
        let validate_span = tracer
            .span_builder("validate_order_input")
            .with_kind(opentelemetry::trace::SpanKind::Internal)
            .start(&tracer);
        let cx = opentelemetry::Context::current_with_span(validate_span);
        let _guard = cx.attach();
        
        tokio::time::sleep(Duration::from_millis(5)).await;
        tracing::debug!("Validating order input parameters");
        println!("   ✓ Sent log: Validating order input");
    }
    
    // Step 3: Query the database
    {
        let db_span = tracer
            .span_builder("fetch_user_data")
            .with_kind(opentelemetry::trace::SpanKind::Internal)
            .start(&tracer);
        let cx = opentelemetry::Context::current_with_span(db_span);
        let _guard = cx.attach();
        
        simulate_db_query().await?;
        println!("   ✓ Sent DB query span and log");
    }
    
    // Step 4: Call payment service (this will fail)
    {
        let payment_span = tracer
            .span_builder("process_payment")
            .with_kind(opentelemetry::trace::SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new("payment.amount", 99.99),
                KeyValue::new("payment.currency", "USD"),
            ])
            .start(&tracer);
        let cx = opentelemetry::Context::current_with_span(payment_span);
        let _guard = cx.attach();
        
        // Log payment attempt
        tracing::info!(amount = 99.99, currency = "USD", "Processing payment for funny order");
        println!("   ✓ Sent log: Processing payment");
        
        // Simulate HTTP call to payment service
        simulate_http_call("payment-service").await?;
        println!("   ✓ Sent HTTP call span and log");
        
        // Simulate an error occurring during payment
        tracing::error!(error_code = "INSUFFICIENT_FUNDS", "Payment gateway returned error: Insufficient funds");
        println!("   ✓ Sent log: Payment error");
        
        // Set the span status to ERROR BEFORE the span ends (this is what makes it show as red in the UI)
        // The status must be set while the span is still recording
        use opentelemetry::trace::{Span, Status};
        opentelemetry::trace::get_active_span(|span| {
            if span.is_recording() {
                span.set_status(Status::error("Payment failed: Insufficient funds"));
                println!("   ✓ Set span status to ERROR (span is recording, status will be exported)");
            } else {
                eprintln!("   ⚠️  WARNING: Span is not recording, cannot set status!");
            }
        });
        
        // Create and capture the error using the Reiver SDK
        // The SDK will automatically extract trace_id from OpenTelemetry context
        let payment_error = PaymentError::new("Insufficient funds in account (error code: INSUFFICIENT_FUNDS)");
        reiver_sdk::capture_exception(&payment_error);
        println!("   ✓ Captured error via Reiver SDK: PaymentError (with auto trace_id correlation)");
    }
    
    // Step 5: Log the failure
    tracing::error!("Order processing failed due to payment error");
    println!("   ✓ Sent log: Order processing failed");
    
    // Give time for data to be exported
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    println!("\n✅ Test scenario completed!");
    println!("\n📊 Summary:");
    println!("   - 1 trace with multiple spans (sent via OTLP to /api/v1/traces)");
    println!("   - {} logs with trace_id correlation (sent via OTLP to /api/v1/logs)", 7);
    println!("   - 1 error captured via Reiver SDK (auto trace_id correlation)");
    println!("\n🔍 To verify in the UI:");
    //println!("   URL: {}/projects/{}/errors", config.api_url, config.project_id);
    println!("   1. Go to Errors page and find 'PaymentError'");
    println!("   2. Click 'Correlated Logs' - should show {} correlated logs", 7);
    println!("   3. Click 'View Trace' - should show the trace waterfall");
    println!("   4. All should share trace_id: {}", trace_id.unwrap_or_default());
    
    Ok(())
}

fn print_usage() {
    println!("Reiver Correlation Test");
    println!();
    println!("Usage:");
    println!("  cargo run -- [options]");
    println!();
    println!("Options:");
    println!("  --api-key KEY        API key for authentication (from project_keys table)");
    println!("                       Default: RzohwTxWGVVM8Vg54ehJulN6AkQz0iJn");
    println!("  --project-id ID      Project ID (UUID) - for display only");
    println!("                       Default: 2c60e43d-e9c0-4275-8091-5387b75622bc");
    println!("  --api-url URL        Reiver API URL (default: http://localhost:3000)");
    println!("  --service NAME       Service name (default: correlation-test-service)");
    println!("  --help               Show this help message");
    println!();
    println!("Examples:");
    println!("  # Use defaults (from generate_realistic_data.py)");
    println!("  cargo run");
    println!();
    println!("  # Custom API key");
    println!("  cargo run -- --api-key YOUR_API_KEY");
    println!();
    println!("  # Custom API URL");
    println!("  cargo run -- --api-url http://your-server:3000");
}

fn parse_args() -> Option<TestConfig> {
    let args: Vec<String> = std::env::args().collect();
    let mut config = TestConfig::default();
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--api-key" => {
                i += 1;
                if i < args.len() {
                    config.api_key = args[i].clone();
                }
            }
            "--project-id" => {
                i += 1;
                if i < args.len() {
                    //config.project_id = args[i].clone();
                }
            }
            "--api-url" => {
                i += 1;
                if i < args.len() {
                    config.api_url = args[i].clone();
                }
            }
            "--service" => {
                i += 1;
                if i < args.len() {
                    config.service_name = args[i].clone();
                }
            }
            "--help" | "-h" => {
                print_usage();
                return None;
            }
            _ => {}
        }
        i += 1;
    }
    
    Some(config)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let config = match parse_args() {
        Some(c) => c,
        None => return Ok(()),
    };
    
    println!("🔧 Configuration:");
    println!("   API URL: {}", config.api_url);
    println!("   Service: {}", config.service_name);
    //println!("   Project ID: {}", config.project_id);
    println!("   API Key: {}...", &config.api_key[..std::cmp::min(10, config.api_key.len())]);
    
    // Initialize Reiver SDK with OpenTelemetry support
    let reiver_guard = reiver_sdk::init((
        config.api_key.as_str(),
        reiver_sdk::ClientOptions {
            api_url: Some(config.api_url.clone()),
            environment: Some("test".to_string()),
            batch_size: 1,
            batch_timeout: Duration::from_secs(1),
            ..Default::default()
        }
    ));
    println!("\n✓ Reiver SDK initialized (with OpenTelemetry trace correlation)");
    
    // Initialize OpenTelemetry tracer for spans (installs global tracer provider)
    let tracer_provider = init_tracer(&config)?;
    println!("✓ OpenTelemetry tracer initialized (OTLP -> /api/v1/traces)");
    
    // Initialize OpenTelemetry logger for logs
    let logger_provider = init_logs(&config)?;
    println!("✓ OpenTelemetry logger initialized (OTLP -> /api/v1/logs)");
    
    // Set up tracing-subscriber with OpenTelemetry layer for logs
    let otel_layer = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);
    
    // Filter to only send our app's logs to OTel, not reqwest/hyper internal logs
    // Include error level so tracing::error! logs are sent
    let filter = tracing_subscriber::filter::EnvFilter::new("correlation_test=debug,info,warn,error");
    
    tracing_subscriber::registry()
        .with(otel_layer.with_filter(filter.clone()))
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .init();
    println!("✓ Tracing subscriber initialized");
    
    // Run the test scenario
run_test_scenario(&config, &reiver_guard).await?;

    // Flush Reiver SDK to ensure error is sent
    println!("\n⏳ Flushing Reiver SDK...");
    let pending = reiver_guard.flush(5).await;
    if pending > 0 {
        eprintln!("Warning: {} events still pending after flush", pending);
    }
    
    // Wait for batch exports
    println!("⏳ Waiting for batch exports...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Shutdown OpenTelemetry providers
    println!("⏳ Shutting down OpenTelemetry...");
    tracer_provider.shutdown()?;
    logger_provider.shutdown()?;
    
    println!("🎉 Done!\n");
    
    Ok(())
}
