//! OTel HTTP server metrics middleware for Axum.
//!
//! Records the three standard metrics from the stable OTel HTTP semantic
//! conventions (v1.23.0+):
//!
//! - `http.server.request.duration`  — histogram (seconds)
//! - `http.server.request.body.size` — histogram (bytes)
//! - `http.server.response.body.size` — histogram (bytes)
//!
//! Attributes: `http.request.method`, `http.route`, `http.response.status_code`.
//!
//! Usage: `.layer(axum::middleware::from_fn(reiver_core::http_metrics::layer))`

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::metrics::Histogram;
use opentelemetry::KeyValue;
use std::sync::OnceLock;
use std::time::Instant;

struct Instruments {
    request_duration: Histogram<f64>,
    request_body_size: Histogram<u64>,
    response_body_size: Histogram<u64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    INSTRUMENTS.get_or_init(|| {
        let meter = opentelemetry::global::meter("http.server");
        Instruments {
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("Duration of HTTP server requests.")
                .with_unit("s")
                .build(),
            request_body_size: meter
                .u64_histogram("http.server.request.body.size")
                .with_description("Size of HTTP server request bodies.")
                .with_unit("By")
                .build(),
            response_body_size: meter
                .u64_histogram("http.server.response.body.size")
                .with_description("Size of HTTP server response bodies.")
                .with_unit("By")
                .build(),
        }
    })
}

pub async fn layer(
    matched_path: Option<MatchedPath>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let inst = instruments();

    let method = request.method().as_str().to_owned();
    let route = matched_path
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| request.uri().path().to_owned());

    let request_size = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let start = Instant::now();
    let response = next.run(request).await;
    let duration = start.elapsed().as_secs_f64();

    let status = response.status().as_u16().to_string();

    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new("http.route", route),
        KeyValue::new("http.response.status_code", status),
    ];

    inst.request_duration.record(duration, &attrs);

    if let Some(size) = request_size {
        inst.request_body_size.record(size, &attrs);
    }

    if let Some(size) = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        inst.response_body_size.record(size, &attrs);
    }

    response
}
