//! OpenTelemetry initialization for Watch.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_PROJECT_ID` are set, Watch
//! exports its own traces and metrics via OTLP HTTP to the configured endpoint
//! (e.g. Jaeger at `http://jaeger.reiver-infra.svc:4318`).
//!
//! Uses standard OTLP paths (`/v1/traces`, `/v1/metrics`). If pointing at
//! Watch's own ingest API, include `/api` in the endpoint URL so the final
//! path becomes `/api/v1/traces`.
//!
//! **No log bridge** -- unlike Flow, Watch omits the
//! `opentelemetry-appender-tracing` layer because Watch *is* the OTLP
//! collector. Bridging tracing logs to OTLP would risk recursion when the
//! SDK's background export tasks emit log events.
//!
//! The feature is opt-in: if the env vars are not set, only console logging is used.

use crate::config::Config;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

pub struct TelemetryProviders {
    pub tracer: Option<SdkTracerProvider>,
    pub meter: Option<SdkMeterProvider>,
}

/// Initialize tracing with optional OpenTelemetry layers for traces and metrics.
///
/// Returns `TelemetryProviders` so the caller can shut them down gracefully.
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

            let tracer = tracer_provider.tracer("reiver-watch");
            let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            let meter_provider = build_meter_provider(endpoint, project_id)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to initialize OpenTelemetry metrics exporter (endpoint={}, project_id={}): {}",
                    endpoint, project_id, e
                ))?;
            opentelemetry::global::set_meter_provider(meter_provider.clone());

            let otel_filter = tracing_subscriber::filter::filter_fn(|metadata| {
                let target = metadata.target();
                !target.contains("kafka_consumer")
                    && !target.contains("kafka_log_consumer")
                    && !target.contains("spans_worker")
                    && !target.contains("metrics_worker")
                    && !target.contains("aggregation_worker")
            });

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer.with_filter(otel_filter))
                .init();

            tracing::info!(
                endpoint = %endpoint,
                project_id = %project_id,
                "OpenTelemetry enabled: traces + metrics (no log bridge)"
            );

            Ok(TelemetryProviders {
                tracer: Some(tracer_provider),
                meter: Some(meter_provider),
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

    let otlp_endpoint = format!("{}/v1/traces", endpoint.trim_end_matches('/'));

    let mut headers = HashMap::new();
    headers.insert("X-Project-Id".to_string(), project_id.to_string());

    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(&otlp_endpoint)
        .with_headers(headers)
        .build()?;

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "reiver-watch"),
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

    let otlp_endpoint = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));

    let mut headers = HashMap::new();
    headers.insert("X-Project-Id".to_string(), project_id.to_string());

    let exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(&otlp_endpoint)
        .with_headers(headers)
        .build()?;

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "reiver-watch"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}
