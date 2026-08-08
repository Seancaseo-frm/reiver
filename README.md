# Reiver

Production AI control plane — OpenTelemetry-native observability and LLM gateway in one. Written in Rust. Self-hostable.

## What is Reiver?

Reiver combines two products into a single platform:

- **Watch** -- Application performance monitoring: distributed tracing, error tracking, log aggregation, real-time metrics, and continuous profiling. Ingests OpenTelemetry natively.
- **Flow** -- Unified LLM gateway: route requests to OpenAI, Anthropic, Google Gemini, AWS Bedrock, and 30+ other providers through one API with automatic failover, semantic caching, prompt management, and cost tracking.

The value of combining these: a single trace shows an API request, the LLM call it made, the tokens consumed, and the dollar cost — all correlated automatically.

## Quick start

### Prerequisites

- Docker and Docker Compose
- Rust (latest stable)
- Node.js 18+

### 1. Start the infrastructure

```bash
make setup   # first time only — creates databases, Kafka topics, runs migrations
make dev     # starts Postgres, ClickHouse, Redis, Redpanda + all services
```

This gives you a fully running instance at `http://localhost:3003`.

### 2. Create a project

Open `http://localhost:3003` in your browser, sign up (or use the seeded dev account `dev@example.com`), and create a project. Copy the project API key.

### 3. Send traces (OpenTelemetry)

Any OpenTelemetry SDK works. Point the OTLP exporter at Reiver:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:3000
export OTEL_EXPORTER_OTLP_HEADERS="x-api-key=YOUR_PROJECT_KEY"
```

Or use the Python SDK:

```python
import reiver

reiver.init(api_key="YOUR_PROJECT_KEY", api_url="http://localhost:3000")

try:
    risky_operation()
except Exception as e:
    reiver.capture_exception(e)
```

### 4. Use the LLM gateway

Point any OpenAI-compatible SDK at Flow. No new SDK needed:

```python
from openai import OpenAI

client = OpenAI(
    api_key="YOUR_PROJECT_KEY",
    base_url="http://localhost:3001/v1"
)

response = client.chat.completions.create(
    model="gpt-4o",  # or claude-3-opus, gemini-pro, llama3, etc.
    messages=[{"role": "user", "content": "Hello!"}]
)
```

To test without paying for API calls, use Ollama locally:

```bash
ollama pull llama3.2
make dev-ollama   # starts Flow pointed at Ollama
```

Or use the gateway mock:

```bash
make gateway-mock   # in one terminal
make dev-mock       # in another — routes to mock instead of real providers
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Watch   | 3000 | APM ingestion and query API |
| Flow    | 3001 | LLM gateway and prompt management |
| Website | 3003 | Auth, billing, dashboard UI |
| MCP     | 3002 | AI agent server (K8s only) |

## Configuration

All services are configured via environment variables. These are set automatically by `make dev`:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reiver
REDIS_URL=redis://localhost:6379
CLICKHOUSE_URL=http://default:@localhost:8123
KAFKA_HOSTS=localhost:19092
JWT_SECRET=dev-secret-change-in-production
ENCRYPTION_KEY=<base64-encoded-32-byte-key>
```

### LLM provider keys

Configure default provider keys so users can route through the gateway without their own keys:

```bash
GATEWAY_DEFAULT_OPENAI_API_KEY=sk-...
GATEWAY_DEFAULT_ANTHROPIC_API_KEY=sk-ant-...
GATEWAY_DEFAULT_GOOGLE_API_KEY=AIza...
```

Or let each project configure their own keys through the dashboard.

## Useful commands

```bash
make dev-watch       # run only the APM service
make dev-flow        # run only the LLM gateway
make dev-website     # run only the auth/dashboard service
make seed            # create a test project and print its API key
make reset-db        # drop and recreate all databases
make test            # run the test suite
make help            # list all available commands
```

## Architecture

```mermaid
graph TD
    OTel["OpenTelemetry SDKs"] --> Watch
    OpenAI["OpenAI-compatible SDKs"] --> Flow
    Browser["Browser"] --> Website
    AIAgents["AI Agents"] --> MCP

    subgraph Reiver
        Website["Website :3003<br/>Auth, Dashboard"]
        Watch["Watch :3000<br/>APM"]
        Flow["Flow :3001<br/>LLM Gateway"]
        MCP["MCP :3002<br/>AI Agent"]
    end

    MCP --> Website
    MCP --> Flow
    MCP --> Watch

    Watch --> Postgres
    Watch --> ClickHouse
    Flow --> Postgres
    Website --> Postgres

    Watch --> Redis
    Watch --> Kafka["Redpanda / Kafka"]

    Flow --> Providers["LLM Providers<br/>OpenAI, Anthropic, Google, Bedrock, Ollama, ..."]
```

## Self-hosting (production)

### Infrastructure requirements

| Component | Purpose | Minimum |
|-----------|---------|---------|
| PostgreSQL 15+ | Primary database | 2 vCPU, 4 GB RAM |
| ClickHouse 24+ | Telemetry storage (traces, logs, metrics) | 4 vCPU, 16 GB RAM |
| Redis 7+ | Caching, rate limiting | 1 vCPU, 2 GB RAM |
| Redpanda / Kafka | Event streaming | 2 vCPU, 4 GB RAM |

### Kubernetes

The `deploy/` directory contains production-ready Kustomize manifests and Argo CD applications:

```
deploy/
  k8s/base/                  # Deployments, services, config
  k8s/overlays/production/   # HPA, PDB, ingress, TLS
  gitops/argocd/             # Argo CD Application manifests
  gitops/infra/              # Postgres, ClickHouse, Redis, Redpanda
```

```bash
# 1. Edit overlays with your domain and image registry
# 2. Create secrets (see deploy/k8s/SECRETS.md)
kubectl apply -k deploy/k8s/overlays/production/
```

See [`deploy/SETUP.md`](deploy/SETUP.md) for the full guide.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md) -- Technical deep-dive: storage architecture, query engine internals, type system
- [`docs/`](docs/) -- Roadmap, integration guides, API references

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Business Source License 1.1 -- see [LICENSE](LICENSE).

You can use, modify, and self-host Reiver freely. The only restriction is that you cannot offer it as a competing hosted service. After 4 years, each version converts to Apache 2.0.
