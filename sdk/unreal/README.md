# Reiver Unreal Engine SDK

Game observability SDK for Unreal Engine with OpenTelemetry v1.39.0 semantic conventions.

## Features

- **Automatic Tick/Frame Time Tracking** - Captures frame rate, tick duration, and percentiles
- **Memory Monitoring** - Tracks physical and virtual memory usage
- **Network Quality Metrics** - RTT, jitter, and packet loss tracking
- **Crash Reporting** - Integration with Unreal's crash handler
- **Session Tracking** - Player session lifecycle with quality metrics
- **Match Tracking** - First-class match/game session entity support
- **Blueprint Support** - Full Blueprint-accessible API

## Supported Platforms

- Windows (Win64)
- macOS
- Linux
- Android
- iOS
- PlayStation 4/5
- Xbox One/Series X|S
- Nintendo Switch

## Requirements

- Unreal Engine 5.0 or later (UE 4.27+ with modifications)

## Installation

### Via Plugin

1. Download the latest release
2. Copy the `Reiver` folder to your project's `Plugins/` directory
3. Regenerate project files
4. Enable the plugin in your `.uproject` file or via Edit > Plugins

### Enable in .uproject

```json
{
    "Plugins": [
        {
            "Name": "Reiver",
            "Enabled": true
        }
    ]
}
```

## Quick Start

### C++ Setup

```cpp
#include "ReiverSubsystem.h"

void AMyGameMode::BeginPlay()
{
    Super::BeginPlay();

    // Get the subsystem
    UReiverSubsystem* Reiver = GetGameInstance()->GetSubsystem<UReiverSubsystem>();

    // Configure
    FReiverConfig Config;
    Config.ApiUrl = TEXT("https://api.reiver.io");
    Config.ProjectKey = TEXT("dh_your_project_key");
    Config.GameName = TEXT("My Game");

    // Initialize
    Reiver->InitializeSDK(Config);
}

void AMyGameMode::EndPlay(const EEndPlayReason::Type EndPlayReason)
{
    if (UReiverSubsystem* Reiver = GetGameInstance()->GetSubsystem<UReiverSubsystem>())
    {
        Reiver->ShutdownSDK();
    }

    Super::EndPlay(EndPlayReason);
}
```

### Blueprint Setup

1. In your Game Instance Blueprint, get the Reiver Subsystem
2. Call "Initialize SDK" with your configuration
3. Call "Shutdown SDK" when the game ends

### Match Tracking

```cpp
// When match starts
Reiver->StartMatch(
    TEXT("match_123"),
    TEXT("ranked"),
    TEXT("dust2")
);

// When match ends
Reiver->EndMatch(TEXT("completed"), TEXT("blue"));
```

### Network Quality Reporting

```cpp
// In your network update
float RttMs = NetworkConnection->GetRoundTripTime();
Reiver->ReportNetworkRtt(RttMs / 1000.0f);  // Convert to seconds

float PacketLoss = NetworkConnection->GetPacketLossRatio();
Reiver->ReportPacketLoss(PacketLoss);
```

## Blueprint API

All functions are exposed to Blueprints under the "Reiver" category:

### Initialization
- **Initialize SDK** - Start the SDK with configuration
- **Shutdown SDK** - Stop the SDK and flush telemetry
- **Is Initialized** - Check if SDK is running

### Match Tracking
- **Start Match** - Begin tracking a match
- **End Match** - End the current match

### Player Tracking
- **Set Player Id** - Set the current player identifier
- **Record Player Join** - Record player joining match
- **Record Player Leave** - Record player leaving match

### Network
- **Report Network Rtt** - Report RTT measurement (seconds)
- **Report Packet Loss** - Report packet loss ratio

### Custom Metrics
- **Record Gauge** - Record a gauge metric
- **Record Histogram** - Record a histogram metric
- **Increment Counter** - Increment a counter

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

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `ApiUrl` | `https://api.reiver.io` | Reiver API endpoint |
| `ProjectKey` | (required) | Project API key |
| `GameName` | (empty) | Name of your game |
| `bEnableTickMetrics` | `true` | Track tick/frame times |
| `bEnableMemoryMetrics` | `true` | Track memory usage |
| `bEnableNetworkMetrics` | `true` | Track network quality |
| `bEnableCrashReporting` | `true` | Capture crashes |
| `MetricsIntervalSeconds` | `1.0` | Metrics collection interval |
| `bSendDeviceId` | `true` | Include device identifier |

## Performance

The SDK is designed for minimal performance impact:

- Uses Unreal's ticker system for non-blocking updates
- Ring buffers for metrics aggregation
- Async HTTP for telemetry export
- Configurable collection intervals

## License

MIT License - see LICENSE file for details.
