#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Reiver Production-Like Local Setup ===${NC}"
echo ""

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=mac;;
    *)          echo -e "${RED}Unsupported OS: $OS${NC}"; exit 1;;
esac

echo -e "Detected platform: ${YELLOW}$PLATFORM${NC}"
echo ""

# =============================================================================
# Install Nomad
# =============================================================================
echo -e "${GREEN}[1/4] Installing Nomad...${NC}"

if command -v nomad &> /dev/null; then
    echo -e "  Nomad already installed: $(nomad version | head -1)"
else
    if [ "$PLATFORM" == "mac" ]; then
        if command -v brew &> /dev/null; then
            brew install nomad
        else
            echo -e "${RED}Homebrew not found. Please install Homebrew first.${NC}"
            exit 1
        fi
    else
        NOMAD_VERSION="1.7.3"
        echo "  Downloading Nomad $NOMAD_VERSION..."
        curl -fsSL "https://releases.hashicorp.com/nomad/${NOMAD_VERSION}/nomad_${NOMAD_VERSION}_linux_amd64.zip" -o /tmp/nomad.zip
        unzip -o /tmp/nomad.zip -d /tmp
        sudo mv /tmp/nomad /usr/local/bin/
        rm /tmp/nomad.zip
    fi
    echo -e "  ${GREEN}Nomad installed: $(nomad version | head -1)${NC}"
fi

# =============================================================================
# Install Coolify
# =============================================================================
echo ""
echo -e "${GREEN}[2/4] Setting up Coolify...${NC}"

if docker ps -a --format '{{.Names}}' | grep -q '^coolify$'; then
    echo "  Coolify container already exists"
    if ! docker ps --format '{{.Names}}' | grep -q '^coolify$'; then
        echo "  Starting Coolify..."
        docker start coolify
    fi
else
    echo "  Creating Coolify data directory..."
    mkdir -p ~/.coolify
    
    echo "  Starting Coolify container..."
    docker run -d \
        --name coolify \
        --restart unless-stopped \
        -p 8000:8000 \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v ~/.coolify:/data \
        ghcr.io/coollabsio/coolify:latest
    
    echo "  Waiting for Coolify to initialize (30 seconds)..."
    sleep 30
fi

echo -e "  ${GREEN}Coolify running at http://localhost:8000${NC}"

# =============================================================================
# Build Reiver Docker Images (all 4 services)
# =============================================================================
echo ""
echo -e "${GREEN}[3/4] Building Reiver Docker images...${NC}"

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

SERVICES="watch flow pond website"
for svc in $SERVICES; do
    echo "  Building reiver-${svc}..."
    docker build -t "reiver/reiver-${svc}:latest" -f "${svc}/Dockerfile" .
    echo -e "  ${GREEN}Image built: reiver/reiver-${svc}:latest${NC}"
done

# =============================================================================
# Create Nomad Variables
# =============================================================================
echo ""
echo -e "${GREEN}[4/4] Preparing Nomad variables...${NC}"

# Generate encryption key
ENCRYPTION_KEY=$(openssl rand -base64 32)

echo "  Variables to set (run after starting Nomad):"
echo ""
echo -e "${YELLOW}# Watch (APM)"
echo "nomad var put nomad/jobs/reiver-watch \\"
echo "  DATABASE_URL=\"postgresql://postgres:postgres@host.docker.internal:5432/reiver\" \\"
echo "  CLICKHOUSE_URL=\"http://default:@host.docker.internal:8123\" \\"
echo "  REDIS_URL=\"redis://host.docker.internal:6379\" \\"
echo "  KAFKA_HOSTS=\"host.docker.internal:19092\" \\"
echo "  JWT_SECRET=\"local-dev-secret-change-in-production\" \\"
echo "  ENCRYPTION_KEY=\"$ENCRYPTION_KEY\""
echo ""
echo "# Flow (LLM Gateway)"
echo "nomad var put nomad/jobs/reiver-flow \\"
echo "  DATABASE_URL=\"postgresql://postgres:postgres@host.docker.internal:5432/reiver\" \\"
echo "  CLICKHOUSE_URL=\"http://default:@host.docker.internal:8123\" \\"
echo "  REDIS_URL=\"redis://host.docker.internal:6379\" \\"
echo "  KAFKA_HOSTS=\"host.docker.internal:19092\" \\"
echo "  JWT_SECRET=\"local-dev-secret-change-in-production\" \\"
echo "  ENCRYPTION_KEY=\"$ENCRYPTION_KEY\""
echo ""
echo "# Pond (Warehouse)"
echo "nomad var put nomad/jobs/reiver-pond \\"
echo "  DATABASE_URL=\"postgresql://postgres:postgres@host.docker.internal:5432/reiver\" \\"
echo "  CLICKHOUSE_URL=\"http://default:@host.docker.internal:8123\" \\"
echo "  REDIS_URL=\"redis://host.docker.internal:6379\" \\"
echo "  KAFKA_HOSTS=\"host.docker.internal:19092\" \\"
echo "  JWT_SECRET=\"local-dev-secret-change-in-production\" \\"
echo "  ENCRYPTION_KEY=\"$ENCRYPTION_KEY\" \\"
echo "  R2_BUCKET=\"warehouse\" \\"
echo "  R2_ENDPOINT=\"http://host.docker.internal:19000\" \\"
echo "  R2_ACCESS_KEY_ID=\"minioadmin\" \\"
echo "  R2_SECRET_ACCESS_KEY=\"minioadmin\""
echo ""
echo "# Website (API Gateway)"
echo "nomad var put nomad/jobs/reiver-website \\"
echo "  DATABASE_URL=\"postgresql://postgres:postgres@host.docker.internal:5432/reiver\" \\"
echo "  REDIS_URL=\"redis://host.docker.internal:6379\" \\"
echo "  JWT_SECRET=\"local-dev-secret-change-in-production\" \\"
echo "  ENCRYPTION_KEY=\"$ENCRYPTION_KEY\" \\"
echo "  WATCH_URL=\"http://host.docker.internal:3000\" \\"
echo "  FLOW_URL=\"http://host.docker.internal:3001\" \\"
echo "  POND_URL=\"http://host.docker.internal:3002\"${NC}"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "Next steps:"
echo ""
echo "1. Open Coolify at http://localhost:8000 and configure databases:"
echo "   - PostgreSQL (port 5432, database: reiver)"
echo "   - ClickHouse (port 8123)"
echo "   - Redis (port 6379)"
echo "   - Redpanda (port 19092)"
echo "   - MinIO (port 19000)"
echo ""
echo "2. Start Nomad in dev mode:"
echo "   nomad agent -dev -bind 0.0.0.0 &"
echo ""
echo "3. Set Nomad variables (commands printed above)"
echo ""
echo "4. Deploy jobs:"
echo "   make prod-local-deploy"
echo ""
echo "5. Access services:"
echo "   - Watch (APM):          http://localhost:3000"
echo "   - Flow (LLM Gateway):   http://localhost:3001"
echo "   - Pond (Warehouse):     http://localhost:3002"
echo "   - Website (Gateway):    http://localhost:3003"
echo "   - Nomad UI:             http://localhost:4646"
echo "   - Coolify:              http://localhost:8000"
