.PHONY: help dev dev-quick dev-ollama gateway-mock seed seed-ollama test-gateway setup infra infra-wait down clean status reset-db argo ci jaeger ssh woodpecker-fix deploy-remote \
	dev-watch dev-flow dev-pond dev-website \
	dev-watch-api dev-watch-workers dev-watch-kafka-consumer \
	dev-watch-kafka-log-consumer dev-watch-alert-worker dev-watch-aggregation-worker \
	dev-website-api dev-website-workers \
	frontend-build frontend-dev frontend-install test test-e2e-watch itest build \
	prod-local-install prod-local-up prod-local-deploy prod-local-status \
	prod-local-down prod-local-clean prod-local-logs

# =============================================================================
# Environment Variables
# =============================================================================

export PATH := /opt/homebrew/bin:$(PATH)

COMMON_ENV = \
	JWT_SECRET=local-dev-jwt-secret-for-development-only \
	ENCRYPTION_KEY=dGhpcy1pcy1hLTMyLWJ5dGUtZGV2LWtleS0xMjM0NTY= \
	REDIS_URL=redis://localhost:6379 \
	CLICKHOUSE_URL=http://default:@localhost:8123 \
	KAFKA_HOSTS=localhost:19092 \
	RATE_LIMIT_UNAUTH_PER_MINUTE=500 \
	RATE_LIMIT_UNAUTH_PER_HOUR=5000 \
	RUST_LOG=info

DATABASE_URL = postgresql://postgres:postgres@localhost:5432/reiver

# Pond dogfooding: set OTEL_PROJECT_ID to a Watch project UUID to enable
# Pond trace export to Watch. Get the UUID from the UI after creating a project.
# Example: make dev OTEL_PROJECT_ID=9a663ac6-7727-4ac4-819c-9b53db19ae8e
OTEL_PROJECT_ID ?= 81b06ad9-1611-4225-8910-67bc13da59ab

WATCH_ENV = $(COMMON_ENV) \
	DATABASE_URL=$(DATABASE_URL)

FLOW_ENV = $(COMMON_ENV) \
	DATABASE_URL=$(DATABASE_URL) \
	OTLP_INGEST_URL=http://localhost:3003/api/watch/ingest

# Platform default API keys — used as fallback when a project has no own key configured.
# This is how OpenRouter-style volume discounts work: set these to your own keys
# and users get access without configuring their own.
# GATEWAY_DEFAULT_OPENAI_API_KEY=sk-...
# GATEWAY_DEFAULT_ANTHROPIC_API_KEY=sk-ant-...
# GATEWAY_DEFAULT_GOOGLE_API_KEY=AIza...
# GATEWAY_DEFAULT_THETA_API_KEY=...

# To test locally without a paid API key, install Ollama (https://ollama.com/),
# run `ollama pull llama3.2`, and use `make dev-ollama` instead of `make dev`.
FLOW_ENV_OLLAMA = $(FLOW_ENV) \
	GATEWAY_OPENAI_BASE_URL=http://localhost:11434/v1

# Gateway mock: run `make gateway-mock` in one terminal, then start the stack with these
# env vars set so Flow calls the mock instead of real providers (no API cost).
# Default gateway keys let projects without their own provider key use the mock
# (client app only needs project API key from make seed). See scripts/gateway-mock/README.md.
FLOW_ENV_MOCK = $(FLOW_ENV) \
	GATEWAY_OPENAI_BASE_URL=http://127.0.0.1:8090 \
	GATEWAY_ANTHROPIC_BASE_URL=http://127.0.0.1:8091 \
	GATEWAY_GOOGLE_BASE_URL=http://127.0.0.1:8092/v1beta \
	GATEWAY_DEFAULT_OPENAI_API_KEY=sk-test-openai \
	GATEWAY_DEFAULT_ANTHROPIC_API_KEY=sk-test-anthropic \
	GATEWAY_DEFAULT_GOOGLE_API_KEY=sk-test-google

POND_ENV = $(COMMON_ENV) \
	DATABASE_URL=$(DATABASE_URL) \
	R2_BUCKET=warehouse \
	R2_ENDPOINT=http://localhost:19000 \
	R2_ACCESS_KEY_ID=minioadmin \
	R2_SECRET_ACCESS_KEY=minioadmin \
	KAFKA_SYNC_JOBS_TOPIC=reiver.sync_jobs \
	PGWIRE_LISTEN_ADDR=0.0.0.0:5433 \
	RUSTFLAGS="--cfg tokio_unstable"

# Conditionally enable OTel dogfooding when OTEL_PROJECT_ID is provided
ifneq ($(OTEL_PROJECT_ID),)
POND_ENV += \
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:3000 \
	OTEL_PROJECT_ID=$(OTEL_PROJECT_ID)
endif

# API key for Pond dogfooding (profiler, SDK). Get from Watch UI after creating a project.
# Example: make dev DATAHIPPO_API_KEY=KFDy6Jl5OoMyDcQ50XkyRbGCK9vOszDF
DATAHIPPO_API_KEY ?= KFDy6Jl5OoMyDcQ50XkyRbGCK9vOszDF
ifneq ($(DATAHIPPO_API_KEY),)
POND_ENV += \
	DATAHIPPO_API_KEY=$(DATAHIPPO_API_KEY) \
	DATAHIPPO_API_URL=http://localhost:3003
endif

WEBSITE_ENV = $(COMMON_ENV) \
	DATABASE_URL=$(DATABASE_URL) \
	WATCH_URL=http://localhost:3000 \
	FLOW_URL=http://localhost:3001 \
	POND_URL=http://localhost:3002

# Redpanda topics used across services (must match defaults in core/src/config.rs)
KAFKA_TOPICS = \
	reiver.exceptions \
	reiver.spans \
	reiver.logs.otlp \
	reiver.logs.unstructured \
	reiver.metrics \
	reiver.warehouse.sync_jobs \
	reiver.llm.chunks \
	reiver.pipeline.events

# =============================================================================
# Help
# =============================================================================

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-30s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# =============================================================================
# Primary Development Commands
# =============================================================================

setup: ## First-time setup: start infra, wait for readiness, create topics and buckets
	@echo "=== First-Time Setup ==="
	@echo ""
	@echo "Starting infrastructure..."
	docker-compose up -d
	@echo ""
	@echo "Waiting for services to initialize..."
	@sleep 15
	@echo ""
	@echo "Verifying ClickHouse Keeper..."
	@docker-compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && echo "  ClickHouse: OK" || echo "  ClickHouse: FAILED"
	@docker-compose exec -T clickhouse clickhouse-client --query "SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 1" >/dev/null 2>&1 && echo "  ClickHouse Keeper: OK" || echo "  ClickHouse Keeper: FAILED (ReplicatedMergeTree won't work)"
	@echo ""
	@echo "Creating Redpanda topics..."
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || echo "  $$topic: may already exist"; \
	done
	@echo ""
	@echo "Creating MinIO warehouse bucket..."
	@docker run --rm --network host minio/mc:latest sh -c "mc alias set local http://localhost:19000 minioadmin minioadmin && mc mb local/warehouse --ignore-existing" 2>/dev/null || echo "  Bucket creation skipped"
	@echo ""
	@echo "Building frontend..."
	@cd website/frontend && npm install && npm run build
	@echo ""
	@echo "Setup complete! Run 'make dev' to start developing."

dev: ## Start everything: infra + frontend build + all 4 services (shared ClickHouse at localhost:8123)
	@echo "=== Starting Reiver Development Environment ==="
	@echo ""
	@echo "Starting infrastructure (Postgres, ClickHouse, Redis, Redpanda, MinIO)..."
	@docker-compose up -d
	@echo "Waiting for ClickHouse HTTP (shared DB at localhost:8123)..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do curl -sf http://localhost:8123/ping >/dev/null 2>&1 && break; sleep 1; done
	@curl -sf http://localhost:8123/ping >/dev/null 2>&1 || (echo "ERROR: ClickHouse not reachable at localhost:8123. Run 'make setup' or ensure docker-compose is up." && exit 1)
	@echo "ClickHouse ready."
	@echo ""
	@echo "Verifying ClickHouse Keeper..."
	@docker-compose exec -T clickhouse clickhouse-client --query "SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 1" >/dev/null 2>&1 || (echo "ERROR: ClickHouse Keeper not running. Run 'make setup' first." && exit 1)
	@echo "Creating Redpanda topics if they don't exist..."
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || true; \
	done
	@echo "Creating MinIO warehouse bucket..."
	@docker run --rm --network host minio/mc:latest sh -c "mc alias set local http://localhost:19000 minioadmin minioadmin && mc mb local/warehouse --ignore-existing" 2>/dev/null || true
	@echo ""
	@echo "Installing frontend dependencies..."
	@cd website/frontend && npm install
	@echo ""
	@echo "=== Building and running migrations first ==="
	@echo "  All services use shared ClickHouse at localhost:8123 (Website runs migrations; Flow/Watch/Pond use same DB)."
	@echo "  Website runs PostgreSQL + ClickHouse migrations on startup."
	@echo "  Other services will start once migrations are complete."
	@echo ""
	@echo "  Frontend (UI):        http://localhost:5173"
	@echo "  Watch  (APM):         http://localhost:3000"
	@echo "  Flow   (LLM Gateway): http://localhost:3001"
	@echo "  Pond   (Warehouse):   http://localhost:3002"
	@echo "  Website (Gateway):    http://localhost:3003"
	@echo ""
	@echo "Gateway mock: active (Flow uses mock providers on 8090/8091/8092 — no API keys required)."
	@echo ""
	@echo "Press Ctrl+C to stop all services."
	@echo ""
	@trap 'kill 0' INT TERM; \
	(cd scripts/gateway-mock && pip install -q -r requirements.txt 2>/dev/null; python server.py) & \
	echo "Waiting for gateway mock..."; \
	for i in 1 2 3 4 5 6 7 8 9 10; do curl -sf http://127.0.0.1:8090/health >/dev/null 2>&1 && break; sleep 1; done; \
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website & \
	WEBSITE_PID=$$!; \
	echo "Waiting for Website to finish migrations (listening on :3003)..."; \
	for i in $$(seq 1 120); do \
		if curl -sf http://localhost:3003/health >/dev/null 2>&1; then break; fi; \
		if ! kill -0 $$WEBSITE_PID 2>/dev/null; then echo "Website process died"; exit 1; fi; \
		sleep 1; \
	done; \
	echo "Website ready — starting remaining services..."; \
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch & \
	(cd flow && $(FLOW_ENV_MOCK) cargo run --bin reiver-flow) & \
	$(POND_ENV) cargo run --manifest-path pond/Cargo.toml --bin reiver-pond & \
	API_URL=http://localhost:3003 npm run dev --prefix website/frontend & \
	wait

dev-quick: ## Start everything without frontend rebuild (faster iteration)
	@echo "=== Starting Reiver (quick mode, no frontend build) ==="
	@echo ""
	@echo "Starting infrastructure..."
	@docker-compose up -d
	@echo "Waiting for ClickHouse HTTP (localhost:8123)..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do curl -sf http://localhost:8123/ping >/dev/null 2>&1 && break; sleep 1; done
	@curl -sf http://localhost:8123/ping >/dev/null 2>&1 || (echo "ERROR: ClickHouse not reachable. Ensure docker-compose is up." && exit 1)
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || true; \
	done
	@docker run --rm --network host minio/mc:latest sh -c "mc alias set local http://localhost:19000 minioadmin minioadmin && mc mb local/warehouse --ignore-existing" 2>/dev/null || true
	@echo ""
	@echo "=== Starting services (Website first for migrations; shared ClickHouse localhost:8123) ==="
	@echo "  Watch  (APM):         http://localhost:3000"
	@echo "  Flow   (LLM Gateway): http://localhost:3001"
	@echo "  Pond   (Warehouse):   http://localhost:3002"
	@echo "  Website (Gateway):    http://localhost:3003"
	@echo ""
	@echo "Gateway mock: active (Flow uses mock providers on 8090/8091/8092 — no API keys required)."
	@echo ""
	@echo "Press Ctrl+C to stop all services."
	@echo ""
	@trap 'kill 0' INT TERM; \
	(cd scripts/gateway-mock && pip install -q -r requirements.txt 2>/dev/null; python server.py) & \
	echo "Waiting for gateway mock..."; \
	for i in 1 2 3 4 5 6 7 8 9 10; do curl -sf http://127.0.0.1:8090/health >/dev/null 2>&1 && break; sleep 1; done; \
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website & \
	WEBSITE_PID=$$!; \
	echo "Waiting for Website to finish migrations (listening on :3003)..."; \
	for i in $$(seq 1 120); do \
		if curl -sf http://localhost:3003/health >/dev/null 2>&1; then break; fi; \
		if ! kill -0 $$WEBSITE_PID 2>/dev/null; then echo "Website process died"; exit 1; fi; \
		sleep 1; \
	done; \
	echo "Website ready — starting remaining services..."; \
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch & \
	(cd flow && $(FLOW_ENV_MOCK) cargo run --bin reiver-flow) & \
	$(POND_ENV) cargo run --manifest-path pond/Cargo.toml --bin reiver-pond & \
	wait

dev-ollama: ## Start dev with Ollama as the local OpenAI-compatible LLM (no paid API keys needed)
	@echo "=== Starting Reiver with Ollama LLM backend ==="
	@echo ""
	@echo "Prerequisites:"
	@echo "  1. Install Ollama: https://ollama.com/"
	@echo "  2. Start Ollama:   ollama serve"
	@echo "  3. Pull a model:   ollama pull llama3.2"
	@echo ""
	@echo "The gateway will route 'openai/*' model requests to http://localhost:11434/v1"
	@echo "Use 'openai/llama3.2' as the model name in your API calls."
	@echo ""
	@curl -sf http://localhost:11434/api/tags >/dev/null 2>&1 || (echo "ERROR: Ollama is not running. Start it with: ollama serve" && exit 1)
	@echo "Ollama: Running"
	@echo ""
	@docker-compose up -d
	@echo "Waiting for ClickHouse HTTP (localhost:8123)..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do curl -sf http://localhost:8123/ping >/dev/null 2>&1 && break; sleep 1; done
	@curl -sf http://localhost:8123/ping >/dev/null 2>&1 || (echo "ERROR: ClickHouse not reachable. Ensure docker-compose is up." && exit 1)
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || true; \
	done
	@echo ""
	@echo "Press Ctrl+C to stop all services."
	@echo ""
	@trap 'kill 0' INT TERM; \
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website & \
	WEBSITE_PID=$$!; \
	echo "Waiting for Website to finish migrations (listening on :3003)..."; \
	for i in $$(seq 1 120); do \
		if curl -sf http://localhost:3003/health >/dev/null 2>&1; then break; fi; \
		if ! kill -0 $$WEBSITE_PID 2>/dev/null; then echo "Website process died"; exit 1; fi; \
		sleep 1; \
	done; \
	echo "Website ready — starting remaining services..."; \
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch & \
	(cd flow && $(FLOW_ENV_OLLAMA) cargo run --bin reiver-flow) & \
	$(POND_ENV) cargo run --manifest-path pond/Cargo.toml --bin reiver-pond & \
	API_URL=http://localhost:3003 npm run dev --prefix website/frontend & \
	wait

gateway-mock: ## Start the gateway mock server (OpenAI/Anthropic/Google on 8090/8091/8092). Run in one terminal; in another, run the stack with FLOW_ENV_MOCK so Flow uses the mock. See scripts/gateway-mock/README.md.
	@cd scripts/gateway-mock && pip install -q -r requirements.txt && python server.py

seed: ## Create a dev user, project, and API key for local gateway testing
	@bash scripts/seed-dev.sh

seed-ollama: ## Seed dev data with Ollama as the LLM provider (no paid API key needed)
	@OLLAMA=1 bash scripts/seed-dev.sh

test-gateway: ## Print a ready-to-run curl command to smoke-test the LLM gateway
	@echo ""
	@echo "=== LLM Gateway Smoke Test ==="
	@echo ""
	@echo "1. Get your project API key from the UI: http://localhost:5173"
	@echo "   (Create a project, then go to Settings → API Keys)"
	@echo ""
	@echo "2. Run the following curl command (replace YOUR_PROJECT_API_KEY):"
	@echo ""
	@echo '   curl -s http://localhost:3003/api/gateway/v1/chat/completions \'
	@echo '     -H "Authorization: Bearer YOUR_PROJECT_API_KEY" \'
	@echo '     -H "Content-Type: application/json" \'
	@echo '     -d '"'"'{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"Say hello"}]}'"'"' | jq .'
	@echo ""
	@echo "   For Ollama (make dev-ollama), use model: openai/llama3.2"
	@echo '   curl -s http://localhost:3003/api/gateway/v1/chat/completions \'
	@echo '     -H "Authorization: Bearer YOUR_PROJECT_API_KEY" \'
	@echo '     -H "Content-Type: application/json" \'
	@echo '     -d '"'"'{"model":"openai/llama3.2","messages":[{"role":"user","content":"Say hello"}]}'"'"' | jq .'
	@echo ""
	@echo "3. Check available models:"
	@echo '   curl -s http://localhost:3003/api/gateway/v1/models \'
	@echo '     -H "Authorization: Bearer YOUR_PROJECT_API_KEY" | jq .'
	@echo ""

# =============================================================================
# Argo CD (remote cluster)
# =============================================================================

KUBECONFIG_DATAHIPPO ?= $(HOME)/.kube/config-reiver
REPO_URL ?=

argo: ## Port-forward Argo CD UI to https://localhost:8080 (uses KUBECONFIG_DATAHIPPO; Ctrl+C to stop)
	@echo "Argo CD UI will be at https://localhost:8080 (accept the self-signed cert)"
	@echo "Admin password: kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath=\"{.data.password}\" | base64 -d && echo"
	@echo ""
	KUBECONFIG=$(KUBECONFIG_DATAHIPPO) kubectl port-forward svc/argocd-server -n argocd 8080:443

ci: ## Port-forward Woodpecker CI UI to http://localhost:8080 (uses KUBECONFIG_DATAHIPPO; Ctrl+C to stop)
	@echo "Woodpecker CI UI will be at http://localhost:8080"
	@echo ""
	KUBECONFIG=$(KUBECONFIG_DATAHIPPO) kubectl port-forward -n woodpecker svc/woodpecker-server 8080:80

jaeger: ## Port-forward Jaeger UI to http://localhost:16686 (trace viewer for Watch ingestion pipeline)
	@echo "Jaeger UI: http://localhost:16686"
	@echo ""
	KUBECONFIG=$(KUBECONFIG_DATAHIPPO) kubectl -n reiver-infra port-forward svc/jaeger 16686:16686

# Server SSH: reads deploy/scripts/.server-credentials (gitignored) for IP, user, and password.
SERVER_CREDENTIALS := deploy/scripts/.server-credentials
SERVER_IP   := $(shell grep -E '^IPv4:'     $(SERVER_CREDENTIALS) 2>/dev/null | sed 's/.*:[[:space:]]*//')
SERVER_USER := $(shell grep -E '^Username:' $(SERVER_CREDENTIALS) 2>/dev/null | sed 's/.*:[[:space:]]*//')

ssh: ## SSH into the deploy server (uses deploy/scripts/.server-credentials; no password prompt if sshpass installed)
	@if [ -z "$(SERVER_IP)" ] || [ -z "$(SERVER_USER)" ]; then \
		echo "Missing deploy/scripts/.server-credentials or IPv4/Username lines."; \
		exit 1; \
	fi
	@if command -v sshpass >/dev/null 2>&1; then \
		sshpass -p "$$(grep -E '^Password:' $(SERVER_CREDENTIALS) 2>/dev/null | sed 's/.*:[[:space:]]*//')" ssh -o StrictHostKeyChecking=accept-new $(SERVER_USER)@$(SERVER_IP); \
	else \
		ssh -o StrictHostKeyChecking=accept-new $(SERVER_USER)@$(SERVER_IP); \
	fi

# =============================================================================
# Infrastructure Management
# =============================================================================

infra: ## Start only Docker infrastructure (databases, Kafka, MinIO)
	@echo "Starting infrastructure..."
	@docker-compose up -d
	@sleep 10
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || true; \
	done
	@docker run --rm --network host minio/mc:latest sh -c "mc alias set local http://localhost:19000 minioadmin minioadmin && mc mb local/warehouse --ignore-existing" 2>/dev/null || true
	@echo "Infrastructure ready."

down: ## Stop all Docker infrastructure
	docker-compose down

status: ## Check status of all infrastructure services
	@echo "=== Docker Containers ==="
	@docker-compose ps
	@echo ""
	@echo "=== ClickHouse Keeper ==="
	@docker-compose exec -T clickhouse clickhouse-client --query "SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 1" 2>/dev/null && echo "Keeper: Running" || echo "Keeper: NOT RUNNING"
	@echo ""
	@echo "=== Replicated Tables ==="
	@docker-compose exec -T clickhouse clickhouse-client --query "SELECT database, table, engine FROM system.tables WHERE engine LIKE '%Replicated%' FORMAT PrettyCompact" 2>/dev/null || echo "No replicated tables found"
	@echo ""
	@echo "=== Redpanda Topics ==="
	@docker-compose exec -T redpanda rpk topic list 2>/dev/null || echo "Redpanda not running"

# =============================================================================
# Database Management
# =============================================================================

reset-db: ## Drop and recreate all databases (WARNING: deletes all data)
	@echo "=== Resetting All Databases ==="
	@echo ""
	@echo "Ensuring Postgres is up..."
	@docker-compose up -d postgres 2>/dev/null || true
	@sleep 3
	@echo ""
	@echo "Dropping and recreating PostgreSQL database..."
	@echo "  Resetting reiver..."
	@docker-compose exec -T postgres psql -U postgres -d postgres -c \
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'reiver' AND pid <> pg_backend_pid();" 2>/dev/null || true
	@docker-compose exec -T postgres psql -U postgres -d postgres -c \
		"DROP DATABASE IF EXISTS reiver WITH (FORCE);" 2>/dev/null || true
	@docker-compose exec -T postgres psql -U postgres -d postgres -c \
		"CREATE DATABASE reiver;" || echo "    FAILED to create reiver"
	@echo ""
	@echo "Dropping and recreating ClickHouse databases..."
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS reiver SYNC;" 2>/dev/null || echo "  ClickHouse not running"
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS catalog SYNC;" 2>/dev/null || true
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP TABLE IF EXISTS default.refinery_schema_history SYNC;" 2>/dev/null || true
	@docker-compose exec -T clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS reiver;" 2>/dev/null || true
	@echo ""
	@echo "Clearing SQLx migration caches..."
	@rm -rf .sqlx watch/.sqlx flow/.sqlx pond/.sqlx website/.sqlx 2>/dev/null || true
	@echo ""
	@echo "Databases reset. Run 'make dev' to apply migrations."

wipe: ## Wipe all ClickHouse table data and Redis stats (keeps table structure)
	@echo "Wiping ClickHouse tables..."
	@docker-compose exec -T clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS reiver.exceptions;" 2>/dev/null || true
	@docker-compose exec -T clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS reiver.exception_groups;" 2>/dev/null || true
	@echo "Wiping Redis stats..."
	@docker-compose exec -T redis redis-cli EVAL "local keys = redis.call('keys', ARGV[1]) for i=1,#keys,5000 do redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) end return keys" 0 "stats:project:*" >/dev/null 2>&1 || true
	@echo "Done."

# =============================================================================
# Individual Service Targets
# =============================================================================

dev-watch: ## Run only Watch (APM) against shared infra
	@echo "Starting Watch (APM) on http://localhost:3000"
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch

dev-flow: ## Run only Flow (LLM Gateway) against shared infra
	@echo "Starting Flow (LLM Gateway) on http://localhost:3001"
	cd flow && $(FLOW_ENV) cargo run --bin reiver-flow

dev-pond: ## Run only Pond (Warehouse) against shared infra
	@echo "Starting Pond (Warehouse) on http://localhost:3002"
	$(POND_ENV) cargo run --manifest-path pond/Cargo.toml --bin reiver-pond

dev-website: ## Run only Website (API Gateway) against shared infra
	@echo "Starting Website (API Gateway) on http://localhost:3003"
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website

# =============================================================================
# Watch Worker Mode Targets
# =============================================================================

dev-watch-api: ## Run Watch in API-only mode (no workers)
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode api

dev-watch-workers: ## Run Watch workers only (no API)
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode workers

dev-watch-kafka-consumer: ## Run Watch Kafka exception consumer only
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode kafka-consumer

dev-watch-kafka-log-consumer: ## Run Watch Kafka log consumer only
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode kafka-log-consumer

dev-watch-alert-worker: ## Run Watch alert evaluation worker only
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode alert-worker

dev-watch-aggregation-worker: ## Run Watch aggregation worker only
	$(WATCH_ENV) cargo run --manifest-path watch/Cargo.toml --bin reiver-watch -- --mode aggregation-worker

# =============================================================================
# Website Worker Mode Targets
# =============================================================================

dev-website-api: ## Run Website in API-only mode (no workers)
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website -- --mode api

dev-website-workers: ## Run Website workers only (no API)
	$(WEBSITE_ENV) cargo run --manifest-path website/Cargo.toml --bin reiver-website -- --mode workers

# =============================================================================
# Frontend
# =============================================================================

frontend-build: ## Build frontend
	cd website/frontend && npm install && npm run build

frontend-dev: ## Start frontend development server
	cd website/frontend && npm run dev

frontend-install: ## Install frontend dependencies
	cd website/frontend && npm install

# =============================================================================
# Build & Test
# =============================================================================

build: ## Build all services (release mode)
	cargo build --release --manifest-path watch/Cargo.toml
	cd flow && cargo build --release
	cargo build --release --manifest-path pond/Cargo.toml
	cargo build --release --manifest-path website/Cargo.toml

build-pond: ## Build pond in release (same as CI). Run before push to catch errors without waiting for CI.
	cargo build --release --manifest-path pond/Cargo.toml

test: ## Run tests for all services
	cargo test --manifest-path watch/Cargo.toml
	cd flow && cargo test
	cargo test --manifest-path pond/Cargo.toml
	cargo test --manifest-path website/Cargo.toml

test-e2e-watch: ## Run Watch E2E tests (requires make dev)
	cargo test --manifest-path watch/Cargo.toml --test e2e_tests -- --ignored --nocapture --test-threads=1

itest: ## Start infra + services, run all integration tests, then tear down
	@echo "=== Reiver Integration Tests ==="
	@echo ""
	@# ── 0. Kill any stale service processes from previous runs ──────────
	@echo "[itest] Killing stale service processes..."
	@kill $$(lsof -ti:3000,3001,3002,3003,5433 2>/dev/null) 2>/dev/null || true
	@sleep 1
	@# ── 1. Start Docker infrastructure ──────────────────────────────────
	@echo "[itest] Starting infrastructure..."
	@docker-compose up -d
	@echo "[itest] Waiting for infrastructure to be ready..."
	@sleep 10
	@# Verify Postgres is healthy -- if template1 collation is broken, recreate the volume
	@if ! docker-compose exec -T postgres psql -U postgres -d postgres -c "SELECT 1" > /dev/null 2>&1; then \
		echo "[itest] Postgres not responding, waiting longer..."; \
		sleep 5; \
	fi
	@if ! docker-compose exec -T postgres psql -U postgres -d template1 -c "SELECT 1" > /dev/null 2>&1; then \
		echo "[itest] WARNING: Postgres template1 is broken (collation mismatch). Recreating volume..."; \
		docker-compose down -v; \
		docker-compose up -d; \
		echo "[itest] Waiting for fresh Postgres to be ready..."; \
		sleep 10; \
	fi
	@echo "[itest] Resetting ClickHouse databases..."
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS reiver SYNC;" 2>/dev/null || true
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS catalog SYNC;" 2>/dev/null || true
	@docker-compose exec -T clickhouse clickhouse-client --query "DROP TABLE IF EXISTS default.refinery_schema_history SYNC;" 2>/dev/null || true
	@echo "[itest] Flushing Redis (rate limits, caches)..."
	@docker-compose exec -T redis redis-cli FLUSHALL 2>/dev/null || true
	@echo "[itest] Creating Kafka topics..."
	@for topic in $(KAFKA_TOPICS); do \
		docker-compose exec -T redpanda rpk topic create $$topic --partitions 3 -c retention.ms=604800000 -c compression.type=snappy --if-not-exists 2>/dev/null || true; \
	done
	@echo "[itest] Creating MinIO bucket..."
	@docker run --rm --network host minio/mc:latest sh -c "mc alias set local http://localhost:19000 minioadmin minioadmin && mc mb local/warehouse --ignore-existing" 2>/dev/null || true
	@echo ""
	@# ── 2. Build services (fail fast before starting anything) ──────────
	@echo "[itest] Building all services..."
	@cargo build --manifest-path website/Cargo.toml 2>&1
	@cargo build --manifest-path watch/Cargo.toml 2>&1
	@cd flow && cargo build 2>&1
	@cargo build --manifest-path pond/Cargo.toml 2>&1
	@echo ""
	@# ── 3. Start Website first (runs all PG + ClickHouse migrations + proxy for other services) ──
	@echo "[itest] Starting Website (port 3003)..."
	@$(WEBSITE_ENV) website/target/debug/reiver-website &
	@printf "  Waiting for Website..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:3003/health > /dev/null 2>&1; then \
			echo " ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo " TIMEOUT"; \
			echo "[itest] ERROR: Website failed to start."; \
			kill $$(lsof -ti:3003 2>/dev/null) 2>/dev/null || true; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo "[itest] Starting Watch (3000), Flow (3001), Pond (3002)..."
	@$(WATCH_ENV) watch/target/debug/reiver-watch &
	@$(FLOW_ENV) flow/target/debug/reiver-flow &
	@$(POND_ENV) pond/target/debug/reiver-pond &
	@echo ""
	@# ── 4. Wait for services to be healthy ──────────────────────────────
	@echo "[itest] Waiting for services to be healthy..."
	@for port in 3000 3001 3002; do \
		printf "  Waiting for port $$port..."; \
		for i in $$(seq 1 60); do \
			if curl -sf http://localhost:$$port/health > /dev/null 2>&1; then \
				echo " ready"; \
				break; \
			fi; \
			if [ $$i -eq 60 ]; then \
				echo " TIMEOUT"; \
				echo "[itest] ERROR: Service on port $$port failed to start."; \
				kill $$(lsof -ti:3000,3001,3002,3003,5433 2>/dev/null) 2>/dev/null || true; \
				exit 1; \
			fi; \
			sleep 2; \
		done; \
	done
	@echo ""
	@# ── 5. Run integration tests ───────────────────────────────────────
	@echo "[itest] Running Watch E2E tests..."
	@cargo test --manifest-path watch/Cargo.toml --test e2e_tests -- --ignored --nocapture --test-threads=1; \
	WATCH_EXIT=$$?; \
	echo ""; \
	echo "[itest] Running Pond tier transition tests..."; \
	DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reiver \
	cargo test --manifest-path pond/Cargo.toml --test tier_transition_tests -- --ignored --nocapture --test-threads=1; \
	POND_EXIT=$$?; \
	echo ""; \
	echo "[itest] Running Flow gateway tests..."; \
	cd flow && cargo test --test gateway_tests --test llm_tests -- --nocapture --test-threads=1; \
	FLOW_EXIT=$$?; \
	echo ""; \
	echo "[itest] Stopping services..."; \
	kill $$(lsof -ti:3000,3001,3002,3003,5433 2>/dev/null) 2>/dev/null || true; \
	sleep 2; \
	if [ $$WATCH_EXIT -ne 0 ]; then \
		echo "[itest] Watch tests FAILED (exit code $$WATCH_EXIT)"; \
		exit $$WATCH_EXIT; \
	fi; \
	if [ $$POND_EXIT -ne 0 ]; then \
		echo "[itest] Pond tests FAILED (exit code $$POND_EXIT)"; \
		exit $$POND_EXIT; \
	fi; \
	if [ $$FLOW_EXIT -ne 0 ]; then \
		echo "[itest] Flow tests FAILED (exit code $$FLOW_EXIT)"; \
		exit $$FLOW_EXIT; \
	fi; \
	echo "[itest] ALL TESTS PASSED"

clean: ## Clean build artifacts and stop containers
	docker-compose down 2>/dev/null || true
	cargo clean --manifest-path watch/Cargo.toml 2>/dev/null || true
	cd flow && cargo clean 2>/dev/null || true
	cargo clean --manifest-path pond/Cargo.toml 2>/dev/null || true
	cargo clean --manifest-path website/Cargo.toml 2>/dev/null || true
	rm -rf website/frontend/.next website/frontend/node_modules

# =============================================================================
# Worker Modes Reference
# =============================================================================

modes: ## List available worker modes for Watch and Website
	@echo "Watch (APM) worker modes (--mode flag):"
	@echo "  all                - Run everything (default)"
	@echo "  api                - HTTP API server only"
	@echo "  workers            - All workers, no API"
	@echo "  kafka-consumer     - Kafka exception consumer"
	@echo "  kafka-log-consumer - Kafka log consumer"
	@echo "  alert-worker       - Alert evaluation"
	@echo "  aggregation-worker - Rate aggregation"
	@echo "  aws-worker         - AWS integration"
	@echo "  azure-worker       - Azure integration"
	@echo "  gcp-worker         - GCP integration"
	@echo "  oci-worker         - OCI integration"
	@echo "  snowflake-worker   - Snowflake integration"
	@echo "  pricing-worker     - LLM pricing sync"
	@echo ""
	@echo "Website worker modes (--mode flag):"
	@echo "  all                - Run everything (default)"
	@echo "  api                - HTTP API server only"
	@echo "  workers            - All workers, no API"
	@echo "  billing-worker     - Billing worker"
	@echo "  auth-event-worker  - Auth event worker"
	@echo "  sso-worker         - SSO worker"

# =============================================================================
# Production-Like Local Mode (Nomad + Coolify)
# =============================================================================

NOMAD_SERVICES = watch flow pond website
NOMAD_JOBS = reiver-watch reiver-watch-workers reiver-flow reiver-pond reiver-website reiver-website-workers

prod-local-install: ## Install Nomad and Coolify, build all Docker images
	@chmod +x deploy/local/install.sh
	@./deploy/local/install.sh

prod-local-up: ## Start production-like local environment (Coolify + Nomad)
	@echo "=== Starting Production-Like Local Environment ==="
	@echo ""
	@# Check if Coolify is running
	@if ! docker ps --format '{{.Names}}' | grep -q '^coolify$$'; then \
		echo "Starting Coolify..."; \
		docker start coolify 2>/dev/null || docker run -d \
			--name coolify \
			--restart unless-stopped \
			-p 8000:8000 \
			-v /var/run/docker.sock:/var/run/docker.sock \
			-v ~/.coolify:/data \
			ghcr.io/coollabsio/coolify:latest; \
		sleep 10; \
	fi
	@echo "Coolify: http://localhost:8000"
	@echo ""
	@# Check if Nomad is running
	@if ! pgrep -x nomad > /dev/null; then \
		echo "Starting Nomad in dev mode..."; \
		nomad agent -dev -bind 0.0.0.0 > /tmp/nomad.log 2>&1 & \
		sleep 5; \
	fi
	@echo "Nomad: http://localhost:4646"
	@echo ""
	@# Set variables
	@echo "Setting Nomad variables..."
	@chmod +x deploy/local/variables-local.sh
	@./deploy/local/variables-local.sh 2>/dev/null || echo "Variables may already be set or Nomad not ready"
	@echo ""
	@echo "=== Environment Ready ==="
	@echo ""
	@echo "Next: Configure databases in Coolify (if not done), then run 'make prod-local-deploy'"

prod-local-deploy: ## Build all Docker images and deploy all 6 Nomad jobs
	@echo "Building Docker images..."
	@for svc in $(NOMAD_SERVICES); do \
		echo "  Building reiver-$${svc}..."; \
		docker build -t "reiver/reiver-$${svc}:latest" -f "$${svc}/Dockerfile" . ; \
	done
	@echo ""
	@echo "Deploying Nomad jobs..."
	@for job_file in watch watch-workers flow pond website website-workers; do \
		echo "  Deploying $${job_file}..."; \
		nomad job run "deploy/nomad/$${job_file}.nomad"; \
	done
	@echo ""
	@echo "Deployment complete! Check status with 'make prod-local-status'"

prod-local-status: ## Check status of all Nomad jobs
	@echo "=== Coolify ==="
	@docker ps --filter name=coolify --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Coolify not running"
	@echo ""
	@echo "=== Nomad Jobs ==="
	@nomad status 2>/dev/null || echo "Nomad not running"
	@echo ""
	@for job in $(NOMAD_JOBS); do \
		echo "=== $$job ==="; \
		nomad job status "$$job" 2>/dev/null | grep -A5 "Allocations" || echo "  Not running"; \
		echo ""; \
	done

prod-local-logs: ## View logs for a Nomad job (usage: make prod-local-logs JOB=reiver-watch)
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make prod-local-logs JOB=<job-name>"; \
		echo ""; \
		echo "Available jobs:"; \
		echo "  reiver-watch"; \
		echo "  reiver-watch-workers"; \
		echo "  reiver-flow"; \
		echo "  reiver-pond"; \
		echo "  reiver-website"; \
		echo "  reiver-website-workers"; \
	else \
		ALLOC_ID=$$(nomad job allocs -json $(JOB) 2>/dev/null | jq -r '.[0].ID'); \
		if [ "$$ALLOC_ID" != "null" ] && [ -n "$$ALLOC_ID" ]; then \
			nomad alloc logs -f $$ALLOC_ID; \
		else \
			echo "No allocations found for job $(JOB)"; \
		fi \
	fi

prod-local-down: ## Stop production-like local environment
	@echo "Stopping Nomad jobs..."
	@for job in $(NOMAD_JOBS); do \
		nomad job stop -purge "$$job" 2>/dev/null || true; \
	done
	@echo "Stopping Nomad..."
	@pkill -x nomad 2>/dev/null || true
	@echo "Stopping Coolify..."
	@docker stop coolify 2>/dev/null || true
	@echo "Production-like local environment stopped."

prod-local-clean: ## Remove all production-like local data (WARNING: deletes Coolify data)
	@echo "This will remove all Coolify data and containers."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(MAKE) prod-local-down
	@docker rm coolify 2>/dev/null || true
	@rm -rf ~/.coolify
	@echo "Cleaned up."
