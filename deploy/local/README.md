# Production-Like Local Development

This guide sets up a full production-like environment on your local machine using Nomad + Coolify. Use this to test production deployments before pushing to Hetzner.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Local Machine                                     │
├─────────────────────────────┬───────────────────────────────────────────────┤
│  Coolify (Docker)           │  Nomad (dev mode)                             │
│  http://localhost:8000      │  http://localhost:4646                        │
├─────────────────────────────┼───────────────────────────────────────────────┤
│                             │                                               │
│  PostgreSQL :5432           │  reiver-watch          :3000               │
│    reiver (shared)       │  reiver-watch-workers                      │
│                             │  reiver-flow           :3001               │
│                             │  reiver-pond           :3002               │
│                             │  reiver-website        :3003               │
│  ClickHouse :8123           │  reiver-website-workers                    │
│                             │  reiver-mcp            :3004               │
│  Redis :6379                │                                               │
│  Redpanda :9092             │                                               │
│  MinIO :19000               │                                               │
└─────────────────────────────┴───────────────────────────────────────────────┘
```

## Prerequisites

- Docker Desktop running
- macOS, Linux, or WSL2 on Windows
- At least 8GB RAM available

## Quick Start

```bash
# Install Nomad and Coolify, build all 5 service images
make prod-local-install

# Start the full production-like stack
make prod-local-up

# View status of all 6 jobs
make prod-local-status

# Stop everything
make prod-local-down
```

## Manual Setup

### Step 1: Install Nomad

**macOS:**
```bash
brew install nomad
```

**Linux:**
```bash
# Download latest Nomad
curl -fsSL https://releases.hashicorp.com/nomad/1.7.3/nomad_1.7.3_linux_amd64.zip -o nomad.zip
unzip nomad.zip
sudo mv nomad /usr/local/bin/
rm nomad.zip

# Verify
nomad version
```

### Step 2: Install Coolify

Coolify runs as a Docker container:

```bash
# Create Coolify data directory
mkdir -p ~/.coolify

# Run Coolify
docker run -d \
  --name coolify \
  --restart unless-stopped \
  -p 8000:8000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v ~/.coolify:/data \
  ghcr.io/coollabsio/coolify:latest
```

Wait 1-2 minutes for Coolify to initialize, then open http://localhost:8000

### Step 3: Configure Databases in Coolify

1. Open Coolify at http://localhost:8000
2. Complete the initial setup wizard
3. Create a new project called "reiver-local"
4. Add databases:

**PostgreSQL:**
- Click "Add New Resource" → PostgreSQL
- Version: 16
- Create 1 shared database: `reiver`
- Username: postgres
- Password: postgres
- Port: 5432

**ClickHouse:**
- Click "Add New Resource" → ClickHouse
- Use official: `clickhouse/clickhouse-server:24.3`
- Port: 8123 (HTTP), 9000 (native)

**Redis:**
- Click "Add New Resource" → Redis
- Version: 7-alpine
- Port: 6379

**Redpanda:**
- Click "Add New Resource" → Docker Compose
- Use this compose snippet:
```yaml
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --mode dev-container
    ports:
      - "9092:9092"
      - "19092:19092"
```

**MinIO:**
- Click "Add New Resource" → Docker Compose
```yaml
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "19000:9000"
      - "19001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
```

### Step 4: Start Nomad in Dev Mode

```bash
# Start Nomad agent in dev mode (single node cluster)
nomad agent -dev -bind 0.0.0.0 -log-level INFO &

# Verify it's running
nomad status
```

### Step 5: Set Nomad Variables

```bash
# Run the helper script to set all variables
chmod +x deploy/local/variables-local.sh
./deploy/local/variables-local.sh
```

Or set manually:

```bash
# Watch (APM)
nomad var put nomad/jobs/reiver-watch \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$(openssl rand -base64 32)"

# Flow (LLM Gateway)
nomad var put nomad/jobs/reiver-flow \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$(openssl rand -base64 32)"

# Pond (Warehouse)
nomad var put nomad/jobs/reiver-pond \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$(openssl rand -base64 32)" \
  R2_BUCKET="warehouse" \
  R2_ENDPOINT="http://host.docker.internal:19000" \
  R2_ACCESS_KEY_ID="minioadmin" \
  R2_SECRET_ACCESS_KEY="minioadmin"

# Website (API Gateway)
nomad var put nomad/jobs/reiver-website \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  REDIS_URL="redis://host.docker.internal:6379" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$(openssl rand -base64 32)" \
  WATCH_URL="http://host.docker.internal:3000" \
  FLOW_URL="http://host.docker.internal:3001" \
  POND_URL="http://host.docker.internal:3002"
```

Note: `host.docker.internal` allows containers to reach services on the host.

### Step 6: Build Docker Images

```bash
# Build all 5 service images
for svc in watch flow pond website mcp; do
  docker build -t "reiver/reiver-${svc}:latest" -f "${svc}/Dockerfile" .
done
```

### Step 7: Deploy Nomad Jobs

```bash
# Deploy all 6 jobs
nomad job run deploy/nomad/watch.nomad
nomad job run deploy/nomad/watch-workers.nomad
nomad job run deploy/nomad/flow.nomad
nomad job run deploy/nomad/pond.nomad
nomad job run deploy/nomad/website.nomad
nomad job run deploy/nomad/website-workers.nomad
```

### Step 8: Verify Deployment

```bash
# Check all jobs are running
nomad status

# View logs for a specific service
nomad alloc logs $(nomad job allocs -json reiver-watch | jq -r '.[0].ID')

# Open Nomad UI
open http://localhost:4646
```

## Using the Stack

### Access Points

| Service | URL |
|---------|-----|
| Watch (APM) API | http://localhost:3000 |
| Flow (LLM Gateway) API | http://localhost:3001 |
| Pond (Warehouse) API | http://localhost:3002 |
| Website (Gateway) | http://localhost:3003 |
| MCP (AI Agent) | http://localhost:3004 |
| Nomad UI | http://localhost:4646 |
| Coolify UI | http://localhost:8000 |
| PostgreSQL | localhost:5432 |
| ClickHouse | localhost:8123 |
| Redis | localhost:6379 |
| Redpanda | localhost:19092 |
| MinIO API | localhost:19000 |
| MinIO Console | localhost:19001 |

### Scaling

```bash
# Scale Watch API to 3 instances
nomad job scale reiver-watch 3

# Scale Website API to 3 instances
nomad job scale reiver-website 3
```

### Viewing Logs

```bash
# Get allocation ID for a service
nomad job allocs reiver-watch

# Follow logs
nomad alloc logs -f <alloc-id>

# Or use Nomad UI for easier log viewing
```

### Redeploying After Code Changes

```bash
# Rebuild all images
for svc in watch flow pond website mcp; do
  docker build -t "reiver/reiver-${svc}:latest" -f "${svc}/Dockerfile" .
done

# Force Nomad to use new images
nomad job run deploy/nomad/watch.nomad
nomad job run deploy/nomad/watch-workers.nomad
nomad job run deploy/nomad/flow.nomad
nomad job run deploy/nomad/pond.nomad
nomad job run deploy/nomad/website.nomad
nomad job run deploy/nomad/website-workers.nomad
```

## Troubleshooting

### Nomad can't pull image

For local development, Nomad should use local Docker images. If not:

```bash
# Check Docker images
docker images | grep reiver

# Ensure Nomad is using Docker driver correctly
nomad agent-info | grep docker
```

### Containers can't reach databases

The issue is usually Docker networking. Use `host.docker.internal` for database URLs:

```bash
# Re-set variables with correct host
./deploy/local/variables-local.sh
```

### Coolify won't start

```bash
# Check Coolify logs
docker logs coolify

# Restart Coolify
docker restart coolify
```

### Nomad job stuck pending

```bash
# Check what's blocking
nomad job status reiver-watch
nomad alloc status <alloc-id>

# Common issues:
# - Not enough resources (increase memory in job spec)
# - Image not found (rebuild with docker build)
```

## Cleanup

```bash
# Stop all Nomad jobs
for job in reiver-watch reiver-watch-workers reiver-flow reiver-pond reiver-website reiver-website-workers reiver-mcp; do
  nomad job stop -purge "$job" 2>/dev/null || true
done

# Stop Nomad
pkill nomad

# Stop Coolify and databases
docker stop coolify
docker rm coolify

# Remove Coolify data (optional)
rm -rf ~/.coolify
```

## Comparison: Dev Mode vs Production-Like Mode

| Aspect | Dev Mode (`make dev`) | Production-Like (`make prod-local-up`) |
|--------|----------------------|----------------------------------------|
| App runs as | `cargo run` on host | Docker containers via Nomad |
| Databases | Docker Compose | Coolify-managed |
| Orchestration | None | Nomad |
| Scaling | N/A | `nomad job scale` |
| Debugging | Easy (host process) | Container logs |
| Production parity | Medium | High |
| Resource usage | Lower | Higher |

Use **Dev Mode** for daily development. Use **Production-Like Mode** to test deployments before pushing to Hetzner.
