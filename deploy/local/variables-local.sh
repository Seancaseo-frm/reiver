#!/bin/bash
# Set Nomad variables for local production-like development.
# Run this after starting Nomad in dev mode.

set -e

echo "Setting Nomad variables for local development..."

# Generate encryption key if not provided
ENCRYPTION_KEY="${ENCRYPTION_KEY:-$(openssl rand -base64 32)}"

# Watch (APM)
echo "  Setting variables for reiver-watch..."
nomad var put nomad/jobs/reiver-watch \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$ENCRYPTION_KEY"

# Flow (LLM Gateway)
echo "  Setting variables for reiver-flow..."
nomad var put nomad/jobs/reiver-flow \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$ENCRYPTION_KEY"

# Pond (Warehouse)
echo "  Setting variables for reiver-pond..."
nomad var put nomad/jobs/reiver-pond \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  CLICKHOUSE_URL="http://default:@host.docker.internal:8123" \
  REDIS_URL="redis://host.docker.internal:6379" \
  KAFKA_HOSTS="host.docker.internal:19092" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$ENCRYPTION_KEY" \
  R2_BUCKET="warehouse" \
  R2_ENDPOINT="http://host.docker.internal:19000" \
  R2_ACCESS_KEY_ID="minioadmin" \
  R2_SECRET_ACCESS_KEY="minioadmin" \
  PGWIRE_LISTEN_ADDR="0.0.0.0:5433"

# Website (API Gateway)
echo "  Setting variables for reiver-website..."
nomad var put nomad/jobs/reiver-website \
  DATABASE_URL="postgresql://postgres:postgres@host.docker.internal:5432/reiver" \
  REDIS_URL="redis://host.docker.internal:6379" \
  JWT_SECRET="local-dev-secret-change-in-production" \
  ENCRYPTION_KEY="$ENCRYPTION_KEY" \
  WATCH_URL="http://host.docker.internal:3000" \
  FLOW_URL="http://host.docker.internal:3001" \
  POND_URL="http://host.docker.internal:3002"

echo ""
echo "Variables set successfully!"
echo ""
echo "Verify with:"
echo "  nomad var get nomad/jobs/reiver-watch"
echo "  nomad var get nomad/jobs/reiver-flow"
echo "  nomad var get nomad/jobs/reiver-pond"
echo "  nomad var get nomad/jobs/reiver-website"
