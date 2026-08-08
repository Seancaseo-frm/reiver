# Application Libraries (SDKs)

> **These libraries are for the user's application to integrate with Reiver.**
> You (the agent) do not install or use these libraries yourself. Use this page when the user asks you to add Reiver to their project — write code that their application will run, using the libraries and patterns below.

Reiver provides client libraries for capturing errors, sending metrics, and integrating observability into applications.

Applications can also use any **OpenTelemetry SDK** to send traces, logs, and metrics to Watch, and any **OpenAI-compatible client library** to use Flow.

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

Capabilities: `capture_exception`, `capture_message`, context/tags/user enrichment, async sending, rate limiting.

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

Capabilities: `capture_exception`, `capture_message`, optional CPU profiling, memory observation, Rayon thread pool instrumentation.

## Unity

Game observability for Unity projects — frame rate, memory usage, network metrics, crash reporting, and match tracking.

## Unreal Engine

Game observability for Unreal Engine projects with full Blueprint support — tick/frame time monitoring, memory tracking, network metrics, crash reporting, session and match tracking.
