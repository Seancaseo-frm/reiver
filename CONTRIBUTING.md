# Contributing to Reiver

Thank you for your interest in contributing to Reiver! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Rust (latest stable)
- Node.js 18+

### Local Development Setup

```bash
make setup   # first time only — builds images, creates Kafka topics
make dev     # starts infrastructure + all services
```

This brings up PostgreSQL, ClickHouse, Redis, and Redpanda in Docker, runs migrations, builds the frontend, and starts all services.

### Useful Commands

```bash
make dev-watch     # run only the APM service
make dev-flow      # run only the LLM gateway
make dev-pond      # run only the warehouse
make reset-db      # drop and recreate all databases
make test          # run tests
make help          # list all available commands
```

## How to Contribute

### Reporting Bugs

- Check existing issues to avoid duplicates
- Use the bug report issue template
- Include steps to reproduce, expected behavior, and actual behavior
- Include your OS, Rust version, and Docker version

### Suggesting Features

- Open a discussion or feature request issue
- Describe the use case and why it would be valuable
- Be open to feedback on alternative approaches

### Submitting Code

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Ensure tests pass: `cargo test --workspace`
5. Ensure code compiles without warnings: `cargo clippy --workspace`
6. Format your code: `cargo fmt --all`
7. Submit a pull request

### Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR
- Write clear commit messages explaining the "why"
- Add tests for new functionality
- Update documentation if behavior changes
- Ensure CI passes before requesting review

## Code Style

- Follow standard Rust conventions (`cargo fmt`)
- Use `cargo clippy` to catch common issues
- Avoid unnecessary `unsafe` code
- Write doc comments for public APIs
- Keep functions focused and reasonably sized

## Project Structure

| Directory | Description |
|-----------|-------------|
| `core/`   | Shared library used by all services |
| `watch/`  | APM service (traces, logs, metrics, errors, profiling) |
| `flow/`   | LLM gateway service |
| `pond/`   | Federated data warehouse |
| `website/`| Auth, billing, dashboard API + frontend |
| `mcp/`    | AI agent MCP server |
| `herd/`   | A2A agent registry |
| `sdk/`    | Client SDKs (Python, Rust, Unity, Unreal) |
| `deploy/` | Kubernetes manifests, CI/CD configs |
| `docs/`   | Documentation site (VitePress) |

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
