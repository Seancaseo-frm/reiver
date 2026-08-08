//! OpenTelemetry initialization for Pond (dogfooding).
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_PROJECT_ID` are set, Pond
//! exports its own traces, metrics, and logs to Watch via OTLP HTTP, enabling
//! us to observe warehouse operations (sync jobs, queries, tier transitions)
//! in our own APM.
//!
//! The feature is opt-in: if the env vars are not set, only console logging is used.

use crate::config::Config;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Holds all OTel providers so main() can shut them down gracefully.
pub struct TelemetryProviders {
    pub tracer: Option<SdkTracerProvider>,
    pub meter: Option<SdkMeterProvider>,
    pub logger: Option<SdkLoggerProvider>,
}

/// Initialize tracing with optional OpenTelemetry layers for traces, metrics,
/// and logs.
///
/// Returns `TelemetryProviders` so the caller can shut them all down gracefully.
///
/// # Errors
///
/// Returns an error if OTel is configured but any exporter fails to
/// initialize. This is intentional -- a misconfigured exporter should
/// prevent deployment, not silently degrade to console-only logging.
pub fn init_telemetry(config: &Config) -> anyhow::Result<TelemetryProviders> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true);

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match (&config.otel_exporter_endpoint, &config.otel_project_id) {
        (Some(endpoint), Some(project_id)) => {
            // --- Traces ---
            let tracer_provider = build_tracer_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry trace exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;

            let tracer = tracer_provider.tracer("reiver-pond");
            let otel_trace_layer = tracing_opentelemetry::layer()
                .with_tracer(tracer);

            // --- Metrics ---
            let meter_provider = build_meter_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry metrics exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;
            opentelemetry::global::set_meter_provider(meter_provider.clone());

            // --- Tokio Runtime Metrics ---
            // Emit Tokio runtime metrics (worker busy time, queue depths, etc.)
            // as OTel metrics. 4 metrics always available; 19 more with
            // RUSTFLAGS="--cfg tokio_unstable".
            opentelemetry_instrumentation_tokio::observe_current_runtime();

            // --- Process Memory + Allocator Metrics ---
            // Emits process.memory.rss, process.memory.virtual, process.memory.page_faults
            // plus jemalloc allocator internals (allocated, active, resident, mapped, retained).
            reiver_sdk::observe_memory()
                .expect("failed to register memory metrics");

            // --- Logs ---
            let logger_provider = build_logger_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry logs exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;
            let otel_log_layer = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer)
                .with(otel_log_layer)
                .init();

            tracing::info!(
                endpoint = %endpoint,
                project_id = %project_id,
                "OpenTelemetry enabled: traces + metrics + logs (dogfooding)"
            );

            Ok(TelemetryProviders {
                tracer: Some(tracer_provider),
                meter: Some(meter_provider),
                logger: Some(logger_provider),
            })
        }
        (Some(_), None) => {
            anyhow::bail!(
                "OTEL_EXPORTER_OTLP_ENDPOINT is set but OTEL_PROJECT_ID is missing. \
                 Set OTEL_PROJECT_ID to a Watch project UUID or unset both."
            );
        }
        _ => {
            // No OTel config -- console logging only (default behavior)
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();

            Ok(TelemetryProviders {
                tracer: None,
                meter: None,
                logger: None,
            })
        }
    }
}

/// Build an OTLP HTTP trace exporter targeting Watch.
fn build_tracer_provider(
    endpoint: &str,
    project_id: &str,
) -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::SpanExporter;
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::Resource;
    use std::collections::HashMap;

    let otlp_endpoint = format!("{}/api/v1/traces", endpoint.trim_end_matches('/'));

    let mut headers = HashMap::new();
    headers.insert("X-Project-Id".to_string(), project_id.to_string());

    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(&otlp_endpoint)
        .with_headers(headers)
        .build()?;

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "reiver-pond"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}

/// Build an OTLP HTTP metrics exporter targeting Watch.
fn build_meter_provider(
    endpoint: &str,
    project_id: &str,
) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::MetricExporter;
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::Resource;
    use std::collections::HashMap;

    let otlp_endpoint = format!("{}/api/v1/metrics", endpoint.trim_end_matches('/'));

    let mut headers = HashMap::new();
    headers.insert("X-Project-Id".to_string(), project_id.to_string());

    let exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(&otlp_endpoint)
        .with_headers(headers)
        .build()?;

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "reiver-pond"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}

/// Build an OTLP HTTP logs exporter targeting Watch.
fn build_logger_provider(
    endpoint: &str,
    project_id: &str,
) -> Result<SdkLoggerProvider, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::LogExporter;
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::Resource;
    use std::collections::HashMap;

    let otlp_endpoint = format!("{}/api/v1/logs", endpoint.trim_end_matches('/'));

    let mut headers = HashMap::new();
    headers.insert("X-Project-Id".to_string(), project_id.to_string());

    let exporter = LogExporter::builder()
        .with_http()
        .with_endpoint(&otlp_endpoint)
        .with_headers(headers)
        .build()?;

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "reiver-pond"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}
