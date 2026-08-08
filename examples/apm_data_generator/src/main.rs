//! Reiver APM Data Generator
//!
//! Generates test data for APM UI features using direct HTTP requests.

use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use rand::Rng;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Generate APM test data for Reiver")]
struct Args {
    /// API key for authentication
    #[arg(long, env = "REIVER_API_KEY")]
    api_key: String,

    /// Reiver API URL
    #[arg(long, default_value = "http://localhost:3000")]
    api_url: String,

    /// Generate all data types
    #[arg(long)]
    all: bool,

    /// Generate traces
    #[arg(long)]
    traces: bool,

    /// Generate logs
    #[arg(long)]
    logs: bool,

    /// Generate errors
    #[arg(long)]
    errors: bool,

    /// Number of traces to generate
    #[arg(long, default_value = "50")]
    trace_count: usize,

    /// Number of logs to generate  
    #[arg(long, default_value = "200")]
    log_count: usize,

    /// Number of errors to generate
    #[arg(long, default_value = "20")]
    error_count: usize,
}

/// Services in our simulated architecture
const SERVICES: &[(&str, &str)] = &[
    ("api-gateway", "2.1.0"),
    ("user-service", "1.5.2"),
    ("payment-service", "3.0.1"),
    ("inventory-service", "1.2.0"),
    ("notification-service", "2.0.0"),
    ("postgres", "15.4"),
    ("redis", "7.2"),
];

/// HTTP endpoints
const ENDPOINTS: &[(&str, &str)] = &[
    ("GET", "/api/v1/users"),
    ("GET", "/api/v1/users/{id}"),
    ("POST", "/api/v1/users"),
    ("GET", "/api/v1/orders"),
    ("POST", "/api/v1/orders"),
    ("POST", "/api/v1/payments"),
    ("GET", "/api/v1/inventory"),
    ("GET", "/api/v1/health"),
];

/// Error types
const ERRORS: &[(&str, &str)] = &[
    ("NullPointerException", "Cannot invoke method on null object"),
    ("ConnectionTimeoutError", "Connection timed out after 30000ms"),
    ("PaymentDeclinedError", "Insufficient funds"),
    ("ValidationError", "email field is required"),
    ("AuthenticationError", "Invalid JWT token"),
    ("DatabaseError", "Deadlock detected"),
];

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("{}", "=".repeat(60).cyan());
    println!("{}", "  Reiver APM Data Generator".cyan().bold());
    println!("{}", "=".repeat(60).cyan());
    println!();
    println!("  API URL: {}", args.api_url.yellow());
    println!("  API Key: {}...", &args.api_key[..8.min(args.api_key.len())].yellow());
    println!();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let generate_all = args.all || (!args.traces && !args.logs && !args.errors);

    if generate_all || args.traces {
        println!("{}", "Generating traces...".green());
        generate_traces(&client, &args).await?;
    }

    if generate_all || args.logs {
        println!("{}", "Generating logs...".green());
        generate_logs(&client, &args).await?;
    }

    if generate_all || args.errors {
        println!("{}", "Generating errors...".green());
        generate_errors(&client, &args).await?;
    }

    println!();
    println!("{}", "Data generation complete!".green().bold());

    Ok(())
}

/// Generate traces via OTLP HTTP JSON
async fn generate_traces(client: &reqwest::Client, args: &Args) -> Result<()> {
    let mut rng = rand::thread_rng();
    let url = format!("{}/api/v1/traces", args.api_url);

    for i in 0..args.trace_count {
        let trace_id = format!("{:032x}", rng.gen::<u128>());
        let service = SERVICES[rng.gen_range(0..SERVICES.len())];
        let endpoint = ENDPOINTS[rng.gen_range(0..ENDPOINTS.len())];
        let status_code = if rng.gen_bool(0.1) { 500 } else { 200 };
        let duration_ns = rng.gen_range(10_000_000..500_000_000i64); // 10ms - 500ms
        let start_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - duration_ns;

        // Create OTLP trace payload
        let payload = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service.0}},
                        {"key": "service.version", "value": {"stringValue": service.1}},
                        {"key": "deployment.environment", "value": {"stringValue": "production"}},
                        {"key": "k8s.namespace.name", "value": {"stringValue": "production"}},
                        {"key": "k8s.pod.name", "value": {"stringValue": format!("{}-pod-{}", service.0, rng.gen::<u32>() % 1000)}},
                        {"key": "k8s.node.name", "value": {"stringValue": "gke-cluster-node-pool-abc123"}}
                    ]
                },
                "scopeSpans": [{
                    "scope": {"name": "apm-generator", "version": "1.0.0"},
                    "spans": [{
                        "traceId": trace_id,
                        "spanId": format!("{:016x}", rng.gen::<u64>()),
                        "name": format!("{} {}", endpoint.0, endpoint.1),
                        "kind": 2,
                        "startTimeUnixNano": start_time.to_string(),
                        "endTimeUnixNano": (start_time + duration_ns).to_string(),
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": endpoint.0}},
                            {"key": "http.route", "value": {"stringValue": endpoint.1}},
                            {"key": "http.status_code", "value": {"intValue": status_code.to_string()}},
                            {"key": "http.request.method", "value": {"stringValue": endpoint.0}},
                            {"key": "url.path", "value": {"stringValue": endpoint.1}},
                            {"key": "http.response.status_code", "value": {"intValue": status_code.to_string()}}
                        ],
                        "status": {
                            "code": if status_code >= 400 { 2 } else { 1 }
                        }
                    }]
                }]
            }]
        });

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-api-key", &args.api_key)
            .json(&payload)
            .send()
            .await;

        if let Err(e) = resp {
            println!("  Warning: Failed to send trace {}: {}", i, e);
        }

        if (i + 1) % 10 == 0 {
            println!("  Generated {}/{} traces", i + 1, args.trace_count);
        }
    }

    Ok(())
}

/// Generate logs via OTLP HTTP JSON
async fn generate_logs(client: &reqwest::Client, args: &Args) -> Result<()> {
    let mut rng = rand::thread_rng();
    let url = format!("{}/api/v1/logs", args.api_url);
    
    let log_messages = [
        ("INFO", "Request received"),
        ("INFO", "Processing user request"),
        ("DEBUG", "Database query executed"),
        ("INFO", "Cache hit for key"),
        ("WARN", "Slow query detected"),
        ("ERROR", "Connection pool exhausted"),
        ("INFO", "User authenticated successfully"),
        ("INFO", "Payment processed"),
        ("WARN", "Rate limit approaching"),
    ];

    for i in 0..args.log_count {
        let service = SERVICES[rng.gen_range(0..SERVICES.len())];
        let (level, message) = log_messages[rng.gen_range(0..log_messages.len())];
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let severity = match level {
            "DEBUG" => 5,
            "INFO" => 9,
            "WARN" => 13,
            "ERROR" => 17,
            _ => 9,
        };

        let payload = json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service.0}},
                        {"key": "service.version", "value": {"stringValue": service.1}}
                    ]
                },
                "scopeLogs": [{
                    "scope": {"name": "apm-generator"},
                    "logRecords": [{
                        "timeUnixNano": timestamp.to_string(),
                        "severityNumber": severity,
                        "severityText": level,
                        "body": {"stringValue": message},
                        "attributes": [
                            {"key": "user_id", "value": {"stringValue": format!("user_{}", rng.gen::<u32>() % 10000)}},
                            {"key": "request_id", "value": {"stringValue": Uuid::new_v4().to_string()}}
                        ]
                    }]
                }]
            }]
        });

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-api-key", &args.api_key)
            .json(&payload)
            .send()
            .await;

        if let Err(e) = resp {
            println!("  Warning: Failed to send log {}: {}", i, e);
        }

        if (i + 1) % 50 == 0 {
            println!("  Generated {}/{} logs", i + 1, args.log_count);
        }
    }

    Ok(())
}

/// Generate errors/exceptions
async fn generate_errors(client: &reqwest::Client, args: &Args) -> Result<()> {
    let mut rng = rand::thread_rng();
    let url = format!("{}/api/v1/exceptions", args.api_url);

    for i in 0..args.error_count {
        let service = SERVICES[rng.gen_range(0..5)];
        let (error_type, error_msg) = ERRORS[rng.gen_range(0..ERRORS.len())];

        let stack_trace = format!(
            "{}: {}\n    at com.example.{}.Handler.process(Handler.java:42)\n    at com.example.{}.Service.execute(Service.java:87)\n    at com.example.core.Router.dispatch(Router.java:156)",
            error_type, error_msg, service.0, service.0
        );

        let payload = json!({
            "type": error_type,
            "message": error_msg,
            "stack_trace": stack_trace,
            "metadata": {
                "service": service.0,
                "version": service.1,
                "environment": "production",
                "user_id": format!("user_{}", rng.gen::<u32>() % 10000),
                "request_id": Uuid::new_v4().to_string()
            },
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-api-key", &args.api_key)
            .json(&payload)
            .send()
            .await;

        if let Err(e) = resp {
            println!("  Warning: Failed to send error {}: {}", i, e);
        }

        if (i + 1) % 5 == 0 {
            println!("  Generated {}/{} errors", i + 1, args.error_count);
        }
    }

    Ok(())
}
