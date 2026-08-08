# Game Development Semantic Conventions

This document defines the OpenTelemetry semantic conventions for game development observability in Reiver, following OTel v1.39.0 patterns.

## Overview

Game development has unique observability requirements that differ from traditional web applications:
- Real-time performance (frame rate, tick rate) is critical
- Client-side performance matters as much as server-side
- Session/match boundaries are meaningful entities
- Network quality varies dramatically across players
- GPU performance is as important as CPU

## Existing OTel Conventions to Leverage

Reiver recognizes and uses these existing OTel v1.39.0 conventions:

### Device Resource Attributes
- `device.id` - Unique device identifier
- `device.manufacturer` - Device manufacturer (e.g., "Samsung", "Apple")
- `device.model.identifier` - Device model identifier
- `device.model.name` - Device model name (e.g., "Galaxy S24", "iPhone 15 Pro")

### Mobile Events (Development Status)
- `device.app.lifecycle` - Application lifecycle events
  - `android.app.state`: `created`, `foreground`, `background`
  - `ios.app.state`: `active`, `inactive`, `foreground`, `background`, `terminate`

### Browser Attributes
- `browser.web_vital` - Web Vitals events (CLS, FID, INP, LCP)
- `browser.mobile` - Boolean indicating mobile browser
- `browser.platform` - Platform name (Windows, macOS, Android, etc.)

### Hardware/GPU Attributes (Development Status)
- `hw.id` - Hardware component identifier
- `hw.vendor` - Hardware vendor (e.g., "NVIDIA", "AMD", "Qualcomm")
- `hw.model` - Hardware model name
- `hw.gpu.task` - GPU task type (`decoder`, `encoder`, `general`)
- `hw.errors` - Hardware error count

---

## Game-Specific Semantic Conventions

### Namespace: `game.*`

All game-specific conventions use the `game.` prefix, following the pattern established by `gen_ai.*` for domain-specific conventions.

---

## Resource Attributes

These attributes identify the game application and should be set once per telemetry source.

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `game.name` | string | Name of the game | `"Cosmic Warfare"` |
| `game.version` | string | Game version | `"2.1.0"` |
| `game.engine` | string | Game engine used | `"Unity"`, `"Unreal"`, `"Godot"` |
| `game.engine.version` | string | Engine version | `"2022.3.15f1"` |
| `game.platform` | string | Target platform | `"pc"`, `"console"`, `"mobile"`, `"web"` |

---

## Span/Metric Attributes

### Match Attributes

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `game.match.id` | string | Unique match identifier | `"match_abc123"` |
| `game.match.mode` | string | Game mode | `"ranked"`, `"casual"`, `"tutorial"` |
| `game.match.map` | string | Map or level name | `"dust2"`, `"level_01"` |
| `game.match.type` | string | Match type | `"pvp"`, `"pve"`, `"coop"` |

### Player Attributes

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `game.player.id` | string | Player identifier (use carefully for privacy) | `"player_xyz"` |
| `game.player.team` | string | Team identifier | `"red"`, `"blue"`, `"1"` |
| `game.session.id` | string | Client session identifier | `"sess_abc123"` |

### Server Attributes

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `game.server.id` | string | Game server identifier | `"srv_001"` |
| `game.server.region` | string | Server geographic region | `"us-west-2"`, `"eu-central-1"` |
| `game.server.instance_type` | string | Server instance type | `"dedicated"`, `"player_hosted"` |

---

## Metrics

All duration metrics use **seconds** (not milliseconds) per OTel v1.39.0 conventions.

### Server Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `game.server.tick.rate` | Gauge | `{Hz}` | Server simulation updates per second |
| `game.server.tick.duration` | Histogram | `s` | Time to process each server tick |
| `game.server.player.count` | Gauge | `{players}` | Active players on server |
| `game.server.match.count` | Gauge | `{matches}` | Active matches on server |
| `game.match.duration` | Histogram | `s` | Total match duration |

### Client Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `game.client.frame.rate` | Gauge | `{Hz}` | Frames per second |
| `game.client.frame.duration` | Histogram | `s` | Time to render each frame |
| `game.client.gpu.duration` | Histogram | `s` | GPU render time per frame |
| `game.client.cpu.duration` | Histogram | `s` | Game logic CPU time per frame |
| `game.client.memory.usage` | Gauge | `By` | Memory consumption in bytes |
| `game.client.scene.load.duration` | Histogram | `s` | Scene/level load time |

### Network Quality Metrics

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `game.network.rtt` | Histogram | `s` | Round-trip time to server |
| `game.network.jitter` | Histogram | `s` | RTT variance |
| `game.network.packet_loss` | Gauge | `1` | Packet loss ratio (0.0-1.0) |
| `game.network.bandwidth` | Gauge | `By/s` | Current throughput in bytes/second |

---

## Events

### Match Lifecycle Events

| Event Name | Description | Attributes |
|------------|-------------|------------|
| `game.match.start` | Match has started | `game.match.id`, `game.match.mode`, `game.match.map`, `game.server.region` |
| `game.match.end` | Match has ended | `game.match.id`, `game.match.duration`, `game.match.outcome` |
| `game.player.join` | Player joined match | `game.match.id`, `game.player.id`, `game.player.team` |
| `game.player.leave` | Player left match | `game.match.id`, `game.player.id`, `game.player.leave_reason` |

### Session Events

| Event Name | Description | Attributes |
|------------|-------------|------------|
| `game.session.start` | Game session started | `game.session.id`, `device.*` attributes |
| `game.session.end` | Game session ended | `game.session.id`, `game.session.duration`, `game.session.end_reason` |

---

## Example Usage

### Python (Server)

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource

# Resource attributes
resource = Resource.create({
    "service.name": "game-server",
    "game.name": "Cosmic Warfare",
    "game.version": "2.1.0",
    "game.engine": "custom",
})

# Tracing
tracer = trace.get_tracer("game.server")

with tracer.start_as_current_span("match.tick") as span:
    span.set_attribute("game.match.id", match_id)
    span.set_attribute("game.match.mode", "ranked")
    span.set_attribute("game.server.region", "us-west-2")
    # ... process tick

# Metrics
meter = metrics.get_meter("game.server")

tick_duration = meter.create_histogram(
    "game.server.tick.duration",
    unit="s",
    description="Time to process each server tick"
)
tick_duration.record(0.016, {"game.match.mode": "ranked"})  # 16ms = 0.016s

player_count = meter.create_gauge(
    "game.server.player.count",
    unit="{players}",
    description="Active players on server"
)
```

### Unity C# (Client)

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;

// Resource attributes
var resource = ResourceBuilder.CreateDefault()
    .AddService("game-client")
    .AddAttributes(new Dictionary<string, object>
    {
        ["game.name"] = "Cosmic Warfare",
        ["game.version"] = Application.version,
        ["game.engine"] = "Unity",
        ["game.engine.version"] = Application.unityVersion,
        ["game.platform"] = GetPlatformString(),
        ["device.manufacturer"] = SystemInfo.deviceModel,
        ["device.model.name"] = SystemInfo.deviceName,
        ["hw.gpu.vendor"] = SystemInfo.graphicsDeviceVendor,
        ["hw.gpu.model"] = SystemInfo.graphicsDeviceName,
    });

// Frame duration metric
var frameDuration = meter.CreateHistogram<double>(
    "game.client.frame.duration",
    unit: "s",
    description: "Time to render each frame"
);

void Update()
{
    frameDuration.Record(Time.deltaTime, new("game.match.id", currentMatchId));
}
```

---

## Compatibility Notes

### OTel v1.39.0 Alignment

- Duration metrics use **seconds** (not milliseconds)
- Ratios use 0.0-1.0 scale (not percentages)
- Byte counts use `By` unit suffix
- Rate metrics use `{unit}/s` or `{unit}` with `/s` suffix

### Stability

These conventions are in **Development** status. To opt-in:

```bash
export OTEL_SEMCONV_STABILITY_OPT_IN=game_experimental
```

### Privacy Considerations

- `game.player.id` should be a pseudonymous identifier, not PII
- Consider using hash of actual user ID
- Session IDs should not correlate across game installations
