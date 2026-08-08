# Reiver Unity SDK

Game observability SDK for Unity with OpenTelemetry v1.39.0 semantic conventions.

## Features

- **Automatic FPS/Frame Time Tracking** - Captures frame rate, frame duration, and percentiles
- **Memory Monitoring** - Tracks allocated, reserved, and managed memory usage
- **Network Quality Metrics** - RTT, jitter, and packet loss tracking
- **Crash Reporting** - Automatic exception and crash capture with fingerprinting
- **Session Tracking** - Player session lifecycle with quality scores
- **Match Tracking** - First-class match/game session entity support
- **Scene Load Tracking** - Automatic scene transition timing

## Installation

### Via Package Manager

1. Open Unity Package Manager (Window > Package Manager)
2. Click "+" > "Add package from git URL"
3. Enter: `https://github.com/reiver/unity-sdk.git`

### Manual Installation

1. Download the latest release
2. Copy the `Reiver` folder to your project's `Packages/` directory

## Quick Start

### 1. Create Configuration

1. Right-click in Project window
2. Select Create > Reiver > Configuration
3. Fill in your API URL and Project Key from Reiver dashboard

### 2. Initialize SDK

```csharp
using Reiver;
using UnityEngine;

public class GameBootstrap : MonoBehaviour
{
    [SerializeField] private ReiverConfig config;

    void Awake()
    {
        ReiverSDK.Initialize(config);
    }

    void OnApplicationQuit()
    {
        ReiverSDK.Shutdown();
    }
}
```

### 3. Track Matches

```csharp
// When match starts
ReiverSDK.StartMatch(
    matchId: "match_123",
    mode: "ranked",
    map: "dust2"
);

// When match ends
ReiverSDK.EndMatch(outcome: "completed", winningTeam: "blue");
```

### 4. Report Network Quality

If using a networking library, report RTT and packet loss:

```csharp
// In your network update loop
float rttMs = networkClient.GetRoundTripTime();
ReiverSDK.ReportNetworkRtt(rttMs / 1000.0); // Convert to seconds

float packetLoss = networkClient.GetPacketLossRatio();
ReiverSDK.ReportPacketLoss(packetLoss);
```

## Semantic Conventions

This SDK follows OpenTelemetry v1.39.0 semantic conventions:

### Metrics

| Metric | Unit | Description |
|--------|------|-------------|
| `game.client.frame.rate` | Hz | Frames per second |
| `game.client.frame.duration` | s | Frame render time |
| `game.client.memory.usage` | By | Memory consumption |
| `game.network.rtt` | s | Round-trip time |
| `game.network.jitter` | s | RTT variance |
| `game.network.packet_loss` | ratio | Packet loss (0.0-1.0) |

### Attributes

| Attribute | Description |
|-----------|-------------|
| `game.match.id` | Current match identifier |
| `game.match.mode` | Game mode (ranked, casual, etc.) |
| `game.match.map` | Map or level name |
| `game.session.id` | Client session identifier |
| `game.player.id` | Player identifier (pseudonymous) |

## Custom Instrumentation

### Custom Metrics

```csharp
// Gauge (point-in-time value)
ReiverSDK.RecordGauge("game.inventory.count", 42);

// Histogram (distribution)
ReiverSDK.RecordHistogram("game.ability.duration", 0.5);

// Counter
ReiverSDK.IncrementCounter("game.kills.total");
```

### Custom Spans

```csharp
using (ReiverSDK.StartSpan("game.ability.cast", new Dictionary<string, string>
{
    ["ability.name"] = "fireball",
    ["ability.level"] = "3"
}))
{
    // Your ability logic here
    CastAbility();
}
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `apiUrl` | `https://api.reiver.io` | Reiver API endpoint |
| `projectKey` | (required) | Project API key |
| `enableFrameMetrics` | `true` | Track FPS/frame times |
| `enableMemoryMetrics` | `true` | Track memory usage |
| `enableNetworkMetrics` | `true` | Track network quality |
| `enableCrashReporting` | `true` | Capture crashes |
| `metricsIntervalSeconds` | `1.0` | Metrics collection interval |
| `batchSize` | `100` | Telemetry batch size |
| `sendDeviceId` | `true` | Include device identifier |

## Performance

The SDK is designed for minimal performance impact:

- **Zero allocation in hot path** - Uses ring buffers for frame timing
- **Background thread for I/O** - Network calls don't block game thread
- **Configurable sampling** - Adjust collection frequency as needed
- **Batched uploads** - Reduces network overhead

## Requirements

- Unity 2021.3 or later
- .NET Standard 2.1 or .NET 4.x

## License

MIT License - see LICENSE file for details.
