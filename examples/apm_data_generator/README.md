# APM Data Generator

Generates comprehensive test data to populate all Reiver APM UI features for testing and demonstration purposes.

## Features

This generator creates data for all APM views:

| Data Type | UI Feature | Description |
|-----------|------------|-------------|
| Traces | Trace Viewer, Flamegraph | Multi-service distributed traces with various scenarios |
| Spans | Service Map | Spans with service attributes for topology visualization |
| HTTP Spans | API Monitoring | Spans with HTTP attributes for endpoint analytics |
| Logs | Log Viewer | Structured logs with trace correlation |
| Exceptions | Exceptions View | Error events with stack traces and fingerprinting |
| Metrics | Metrics Explorer | Counter, gauge, and histogram metric data |
| K8s Attributes | Infrastructure | Pod, node, and deployment resource attributes |

## Prerequisites

1. Reiver server running (default: `http://localhost:3000`)
2. A project created in Reiver
3. An API key from Project Settings

## Installation

```bash
cd examples/apm_data_generator
cargo build --release
```

## Usage

### Get Your API Key

1. Log into Reiver
2. Go to Project Settings > API Keys
3. Copy your API key

### Generate All Data Types

```bash
cargo run --release -- --api-key YOUR_API_KEY --all
```

### Generate Specific Data Types

```bash
# Only traces
cargo run --release -- --api-key YOUR_API_KEY --traces

# Only logs
cargo run --release -- --api-key YOUR_API_KEY --logs

# Only errors
cargo run --release -- --api-key YOUR_API_KEY --errors

# Traces and logs together
cargo run --release -- --api-key YOUR_API_KEY --traces --logs
```

### Customize Volume

```bash
# Generate 100 traces, 1000 logs, 50 errors
cargo run --release -- --api-key YOUR_API_KEY --all \
  --trace-count 100 \
  --log-count 1000 \
  --error-count 50
```

### Continuous Mode

For live testing, run continuously:

```bash
# Generate data every 10 seconds
cargo run --release -- --api-key YOUR_API_KEY --all --continuous

# Custom interval (5 seconds)
cargo run --release -- --api-key YOUR_API_KEY --all --continuous --interval 5
```

### Custom API URL

```bash
cargo run --release -- --api-key YOUR_API_KEY --api-url https://your-reiver.com --all
```

## Trace Scenarios

The generator creates 7 different trace scenarios to test various UI features:

1. **Happy Path** - Normal successful request with standard latency
2. **Slow Trace** - High-latency operations (tests P99 visualization)
3. **Error Trace** - Failed requests with error status codes
4. **Deep Trace** - 8-15 nested spans (tests flamegraph depth)
5. **Multi-Service** - Spans across 5 services (tests service map)
6. **Database Trace** - PostgreSQL query spans with `db.*` attributes
7. **HTTP Trace** - Full HTTP attributes for API monitoring

## Simulated Architecture

The generator simulates this microservices architecture:

```
                    ┌─────────────────┐
                    │   api-gateway   │
                    └────────┬────────┘
           ┌─────────────────┼─────────────────┐
           │                 │                 │
    ┌──────┴──────┐   ┌──────┴──────┐   ┌──────┴──────┐
    │user-service │   │payment-svc  │   │inventory-svc│
    └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
           │                 │                 │
    ┌──────┴──────┐   ┌──────┴──────┐   ┌──────┴──────┐
    │  postgres   │   │ stripe-api  │   │  mongodb    │
    └─────────────┘   └─────────────┘   └─────────────┘
                             │
                      ┌──────┴──────┐
                      │notification │
                      │  -service   │
                      └──────┬──────┘
                      ┌──────┴──────┐
                      │    redis    │
                      └─────────────┘
```

## Kubernetes Attributes

All traces include K8s resource attributes:

- `k8s.namespace.name` - Namespace (production, databases)
- `k8s.pod.name` - Pod name
- `k8s.node.name` - Node name
- `cloud.provider` - Cloud provider (gcp)
- `cloud.region` - Region (us-central1)

These enable the Infrastructure monitoring view to show pod, node, and deployment data.

## Error Types

The generator creates 10 different error types with realistic stack traces:

- NullPointerException
- ConnectionTimeoutError
- PaymentDeclinedError
- ValidationError
- AuthenticationError
- RateLimitError
- ResourceNotFoundError
- DatabaseError
- ExternalServiceError
- OutOfMemoryError

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-key` | API key for authentication (required) | - |
| `--api-url` | Reiver API URL | `http://localhost:3000` |
| `--all` | Generate all data types | false |
| `--traces` | Generate traces | false |
| `--logs` | Generate logs | false |
| `--errors` | Generate errors | false |
| `--metrics` | Generate metrics | false |
| `--trace-count` | Number of traces to generate | 50 |
| `--log-count` | Number of logs to generate | 500 |
| `--error-count` | Number of errors to generate | 30 |
| `--continuous` | Run continuously | false |
| `--interval` | Seconds between batches in continuous mode | 10 |

## Environment Variables

You can also set the API key via environment variable:

```bash
export REIVER_API_KEY=your_api_key
cargo run --release -- --all
```

## Troubleshooting

### Connection Refused

Make sure Reiver server is running:

```bash
# From project root
make dev
```

### Invalid API Key

Get a fresh API key from Project Settings in the Reiver UI.

### No Data Appearing

1. Check the server logs for errors
2. Verify the API key has write permissions
3. Ensure ClickHouse is running and healthy
