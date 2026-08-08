//! OpenTelemetry initialization for Herd.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_PROJECT_ID` are set, Herd
//! exports its own traces, metrics, and logs to Watch via OTLP HTTP.
//!
//! Opt-in: if the env vars are not set, only console logging is used.

use crate::config::Config;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct TelemetryProviders {
    pub tracer: Option<SdkTracerProvider>,
    pub meter: Option<SdkMeterProvider>,
    pub logger: Option<SdkLoggerProvider>,
}

pub fn init_telemetry(config: &Config) -> anyhow::Result<TelemetryProviders> {
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match (&config.otel_exporter_endpoint, &config.otel_project_id) {
        (Some(endpoint), Some(project_id)) => {
            opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

            let tracer_provider = build_tracer_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!("Failed to init OTel trace exporter: {}", e))?;
            let tracer = tracer_provider.tracer("reiver-herd");
            let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            let meter_provider = build_meter_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!("Failed to init OTel metrics exporter: {}", e))?;
            opentelemetry::global::set_meter_provider(meter_provider.clone());

            let logger_provider = build_logger_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!("Failed to init OTel logs exporter: {}", e))?;
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
            KeyValue::new("service.name", "reiver-herd"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
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
            KeyValue::new("service.name", "reiver-herd"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    Ok(SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build())
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
            KeyValue::new("service.name", "reiver-herd"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    Ok(SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}
