# Reiver Agent

Standalone daemon that collects system and database metrics and sends them to the Reiver platform.

## Features

- ✅ **System Metrics**: CPU, memory, disk, network
- ✅ **Database Metrics**: PostgreSQL (pg_stat_statements), MySQL (coming soon)
- ✅ **Configurable**: TOML-based configuration
- ✅ **Environment Variables**: Support for `${VAR_NAME}` substitution in config
- ✅ **Single Daemon**: All collectors run in one process

## Quick Start

### Installation

```bash
# Build the agent
cargo build --release

# The binary will be at: target/release/reiver-agent
```

### Configuration

1. Copy the example config:
```bash
cp configs/reiver.toml.example /etc/reiver/reiver.toml
```

2. Edit the config file:
```bash
vim /etc/reiver/reiver.toml
```

3. Set required environment variables:
```bash
export REIVER_API_KEY="your-api-key-here"
```

### Running

```bash
# Validate configuration
reiver-agent validate --config /etc/reiver/reiver.toml

# Run in foreground (for testing)
reiver-agent run --config /etc/reiver/reiver.toml --foreground

# Run as daemon (background)
reiver-agent run --config /etc/reiver/reiver.toml
```

## Configuration

See `configs/reiver.toml.example` for a complete configuration example.

### Environment Variable Substitution

You can use environment variables in your config using `${VAR_NAME}` syntax:

```toml
[api]
api_key = "${REIVER_API_KEY}"  # Will be replaced with actual env var value
```

## Development

```bash
# Run tests
cargo test

# Run in development mode
cargo run -- run --config configs/reiver.toml.example --foreground
```

## Architecture

The agent follows a modular collector pattern:

- **Agent**: Main orchestrator, manages collectors and HTTP client
- **Collectors**: Trait-based collectors (SystemMetricsCollector, etc.)
- **HTTP Client**: Sends metrics to Reiver API
- **Config**: TOML-based configuration with validation

## Status

🚧 **Early Development**

- ✅ Basic structure and CLI
- ✅ Config loading (TOML)
- ✅ System metrics collection (CPU, memory, disk, network)
- ✅ HTTP client for sending metrics
- 🚧 Database collectors (PostgreSQL, MySQL)
- 🚧 Service installation (systemd, launchd, Windows)
- 🚧 Retry logic and error handling

## License

MIT
