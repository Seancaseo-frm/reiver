//! OTel metrics for the MCP server.
//!
//! Instruments from the global `MeterProvider` are created once and accessed
//! via `McpMetrics::global()`. All counters/histograms use the `mcp.*`
//! namespace.

use opentelemetry::metrics::{Counter, Histogram};
use std::sync::OnceLock;

pub struct McpMetrics {
    pub request_count: Counter<u64>,
    pub request_duration_ms: Histogram<f64>,
    pub tool_call_count: Counter<u64>,
    pub tool_call_duration_ms: Histogram<f64>,
    pub auth_failure: Counter<u64>,
}

static INSTANCE: OnceLock<McpMetrics> = OnceLock::new();

impl McpMetrics {
    pub fn global() -> &'static McpMetrics {
        INSTANCE.get_or_init(Self::new)
    }

    fn new() -> Self {
        let meter = opentelemetry::global::meter("reiver-mcp");

        Self {
            request_count: meter
                .u64_counter("mcp.request.count")
                .with_description("Total JSON-RPC requests")
                .build(),
            request_duration_ms: meter
                .f64_histogram("mcp.request.duration_ms")
                .with_description("JSON-RPC request latency in milliseconds")
                .build(),
            tool_call_count: meter
                .u64_counter("mcp.tool.call.count")
                .with_description("Tool invocations")
                .build(),
            tool_call_duration_ms: meter
                .f64_histogram("mcp.tool.call.duration_ms")
                .with_description("Tool execution time in milliseconds")
                .build(),
            auth_failure: meter
                .u64_counter("mcp.auth.failure")
                .with_description("Authentication failures")
                .build(),
        }
    }
}
