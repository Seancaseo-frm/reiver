# SDKs

Reiver provides client libraries for capturing errors, sending metrics, and integrating observability into your applications and games.

::: info
Detailed SDK documentation is coming soon. This page provides an overview of available SDKs.

In the meantime, you can use any **OpenTelemetry SDK** to send traces, logs, and metrics to [Watch](/watch/), and any **OpenAI-compatible client library** (Python `openai`, Node `openai`, etc.) to use [Flow](/flow/getting-started).
:::

## Python

Error monitoring and exception tracking for Python applications.

```python
import reiver

reiver.init(api_key="dh_your_key", api_url="https://reiver.ai")

try:
    risky_operation()
except Exception as e:
    reiver.capture_exception(e)
```

**Capabilities:** `capture_exception`, `capture_message`, context/tags/user enrichment, async sending, rate limiting.

## Rust

Error monitoring with optional continuous profiling for Rust applications.

```rust
use reiver::Reiver;

let _guard = Reiver::init(reiver::Config {
    dsn: "dh_your_key".into(),
    api_url: "https://reiver.ai".into(),
    ..Default::default()
});

reiver::capture_message("Something happened");
```

**Capabilities:** `capture_exception`, `capture_message`, optional CPU profiling, memory observation, Rayon thread pool instrumentation.

## Unity

Game observability for Unity projects — frame rate, memory usage, network metrics, crash reporting, and match tracking.

**Capabilities:** FPS/frame time monitoring, memory tracking, network RTT/jitter/packet loss, automatic crash reporting, session and match lifecycle tracking.

## Unreal Engine

Game observability for Unreal Engine projects with full Blueprint support.

**Capabilities:** Tick/frame time monitoring, memory tracking, network metrics, crash reporting, session and match tracking, Blueprint API for non-C++ workflows.
