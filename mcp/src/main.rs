#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;

use clap::{Parser, ValueEnum};
use tower_http::trace::TraceLayer;

use reiver_mcp::actions;
use reiver_mcp::registry::ActionRegistry;
use reiver_mcp::telemetry;

/// Reiver MCP Server
#[derive(Parser, Debug)]
#[command(name = "reiver-mcp")]
#[command(about = "MCP server exposing the Reiver platform to AI agents")]
struct Cli {
    /// Transport to use
    #[arg(long, value_enum, default_value = "stdio")]
    transport: Transport,

    /// API key for authentication (required for stdio transport only)
    #[arg(long)]
    api_key: Option<String>,

    /// Listen address for HTTP transport
    #[arg(long, default_value = "0.0.0.0:3002")]
    listen: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Transport {
    /// stdio transport (for local agents like Cursor)
    Stdio,
    /// HTTP transport (for deployment behind the website proxy)
    Http,
}

/// Wraps an `axum::http::HeaderMap` so `opentelemetry::propagation::Extractor`
/// can pull W3C `traceparent` / `tracestate` from incoming HTTP requests.
struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let providers = telemetry::init_telemetry()?;

    let cli = Cli::parse();

    let website_url = std::env::var("WEBSITE_URL").unwrap_or_else(|_| "http://localhost:80".into());
    let flow_url = std::env::var("FLOW_URL").unwrap_or_else(|_| "http://localhost:3001".into());
    let watch_url = std::env::var("WATCH_URL").unwrap_or_else(|_| "http://localhost:3003".into());

    let mut registry = ActionRegistry::new();
    actions::register_all(&mut registry);
    let registry = Arc::new(registry);

    match cli.transport {
        Transport::Stdio => {
            let api_key = cli
                .api_key
                .or_else(|| std::env::var("REIVER_API_KEY").ok())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "API key required for stdio transport (--api-key or REIVER_API_KEY)"
                    )
                })?;

            let context =
                reiver_mcp::auth::authenticate(&api_key, &website_url, &flow_url, &watch_url)
                    .await?;

            tracing::info!(
                project_id = %context.project_id,
                "MCP stdio server starting"
            );

            let server = reiver_mcp::server::McpServer::new(registry, context);
            let stdin = tokio::io::stdin();
            let stdout = tokio::io::stdout();
            let running = rmcp::serve_server(server, (stdin, stdout)).await?;
            running.waiting().await?;
        }
        Transport::Http => {
            use reiver_mcp::http::{handle_mcp, McpHttpState};

            let http_client = reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(30))
                .build()?;

            let state = Arc::new(McpHttpState {
                registry,
                http_client,
                website_url,
                flow_url,
                watch_url,
                meter_service: None,
                db: None,
            });

            let router = axum::Router::new()
                .route("/mcp", axum::routing::post(handle_mcp))
                .with_state(state)
                .layer(axum::middleware::from_fn(
                    reiver_core::http_metrics::layer,
                ))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(|request: &axum::http::Request<_>| {
                            let span = tracing::info_span!(
                                "http.request",
                                otel.kind = "server",
                                otel.status_code = tracing::field::Empty,
                                http.method = %request.method(),
                                http.target = %request.uri(),
                                http.status_code = tracing::field::Empty,
                            );

                            let parent_cx =
                                opentelemetry::global::get_text_map_propagator(|prop| {
                                    prop.extract(&HeaderExtractor(request.headers()))
                                });
                            use tracing_opentelemetry::OpenTelemetrySpanExt;
                            let _ = span.set_parent(parent_cx);

                            span
                        })
                        .on_response(
                            |response: &axum::http::Response<_>,
                             latency: std::time::Duration,
                             span: &tracing::Span| {
                                let status = response.status().as_u16();
                                span.record("http.status_code", status);
                                if status >= 500 {
                                    span.record("otel.status_code", "ERROR");
                                }
                                tracing::info!(
                                    latency_ms = latency.as_millis() as u64,
                                    status,
                                    "response"
                                );
                            },
                        ),
                );

            let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
            tracing::info!(listen = %cli.listen, "MCP HTTP server listening");
            axum::serve(listener, router).await?;
        }
    }

    shutdown_telemetry(providers);

    Ok(())
}

fn shutdown_telemetry(providers: telemetry::TelemetryProviders) {
    if let Some(tp) = providers.tracer {
        if let Err(e) = tp.shutdown() {
            eprintln!("Error shutting down tracer provider: {e}");
        }
    }
    if let Some(mp) = providers.meter {
        if let Err(e) = mp.shutdown() {
            eprintln!("Error shutting down meter provider: {e}");
        }
    }
    if let Some(lp) = providers.logger {
        if let Err(e) = lp.shutdown() {
            eprintln!("Error shutting down logger provider: {e}");
        }
    }
}
