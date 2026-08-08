//! OpenTelemetry initialization for Flow.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_PROJECT_ID` are set, Flow
//! exports its own traces, metrics, and logs to Watch via OTLP HTTP, enabling
//! observability of LLM gateway operations (routing, provider calls, token
//! usage) in our own APM.
//!
//! The feature is opt-in: if the env vars are not set, only console logging is used.

use crate::config::Config;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
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
pub fn init_telemetry(config: &Config) -> anyhow::Result<TelemetryProviders> {
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match (&config.otel_exporter_endpoint, &config.otel_project_id) {
        (Some(endpoint), Some(project_id)) => {
            opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

            let tracer_provider = build_tracer_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry trace exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;

            let tracer = tracer_provider.tracer("reiver-flow");
            let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            let meter_provider = build_meter_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry metrics exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;
            opentelemetry::global::set_meter_provider(meter_provider.clone());

            let logger_provider = build_logger_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry logs exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;
            let otel_log_layer =
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    &logger_provider,
                );

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer)
                .with(otel_log_layer)
                .init();

            tracing::info!(
                endpoint = %endpoint,
                project_id = %project_id,
                "OpenTelemetry enabled: traces + metrics + logs"
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
            KeyValue::new("service.name", "reiver-flow"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}

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
            KeyValue::new("service.name", "reiver-flow"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}

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
            KeyValue::new("service.name", "reiver-flow"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}
