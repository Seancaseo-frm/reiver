#!/usr/bin/env bash
# seed-dev.sh — Bootstrap a local dev account, project, and API key for gateway testing.
#
# Usage:
#   ./scripts/seed-dev.sh                    # basic setup, no LLM provider configured
#   OPENAI_API_KEY=sk-... ./scripts/seed-dev.sh   # also configures OpenAI provider
#   OLLAMA=1 ./scripts/seed-dev.sh           # configure Ollama as the provider (no real API key)
#
# Prerequisites: make dev (or make dev-ollama) must already be running.
# Requires: curl, jq

set -euo pipefail

WEBSITE_URL="${WEBSITE_URL:-http://localhost:3003}"
DEV_EMAIL="${DEV_EMAIL:-dev@reiver.local}"
DEV_PASSWORD="${DEV_PASSWORD:-devpassword123}"
PROJECT_NAME="${PROJECT_NAME:-Dev LLM Project}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()    { echo -e "${CYAN}[seed]${NC} $*"; }
success() { echo -e "${GREEN}[seed]${NC} $*"; }
warn()    { echo -e "${YELLOW}[seed]${NC} $*"; }
error()   { echo -e "${RED}[seed]${NC} $*" >&2; }

# ─── Preflight ────────────────────────────────────────────────────────────────

command -v curl >/dev/null 2>&1 || { error "curl is required"; exit 1; }
command -v jq   >/dev/null 2>&1 || { error "jq is required (brew install jq)"; exit 1; }

info "Checking that the website is reachable at ${WEBSITE_URL}..."
if ! curl -sf "${WEBSITE_URL}/health" >/dev/null 2>&1; then
    error "Website is not responding at ${WEBSITE_URL}. Is 'make dev' running?"
    exit 1
fi
success "Website is up."

# ─── Auth: sign up or log in ──────────────────────────────────────────────────

info "Signing up as ${DEV_EMAIL}..."
SIGNUP_RESP=$(curl -s -w "\n%{http_code}" -X POST "${WEBSITE_URL}/api/auth/signup" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"${DEV_EMAIL}\",\"password\":\"${DEV_PASSWORD}\"}")

SIGNUP_BODY=$(echo "$SIGNUP_RESP" | sed '$d')
SIGNUP_STATUS=$(echo "$SIGNUP_RESP" | tail -n 1)

if [ "$SIGNUP_STATUS" = "200" ] || [ "$SIGNUP_STATUS" = "201" ]; then
    JWT=$(echo "$SIGNUP_BODY" | jq -r '.token // .access_token // empty')
    success "Signed up successfully."
elif echo "$SIGNUP_BODY" | grep -qi "already registered\|already exists"; then
    warn "Account already exists, logging in..."
    LOGIN_RESP=$(curl -s -X POST "${WEBSITE_URL}/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"email\":\"${DEV_EMAIL}\",\"password\":\"${DEV_PASSWORD}\"}")
    JWT=$(echo "$LOGIN_RESP" | jq -r '.token // .access_token // empty')
    if [ -z "$JWT" ] || [ "$JWT" = "null" ]; then
        error "Login failed. Response: $LOGIN_RESP"
        exit 1
    fi
    success "Logged in."
else
    error "Unexpected signup response (HTTP $SIGNUP_STATUS): $SIGNUP_BODY"
    exit 1
fi

if [ -z "$JWT" ] || [ "$JWT" = "null" ]; then
    error "Could not extract JWT from response: $SIGNUP_BODY"
    exit 1
fi

# ─── Create project ───────────────────────────────────────────────────────────

info "Creating project '${PROJECT_NAME}'..."
PROJECT_RESP=$(curl -s -X POST "${WEBSITE_URL}/api/projects" \
    -H "Authorization: Bearer ${JWT}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${PROJECT_NAME}\"}")

PROJECT_ID=$(echo "$PROJECT_RESP" | jq -r '.id // empty')
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "null" ]; then
    # Project might already exist — list projects and pick the first match
    warn "Could not create project (may already exist). Looking up existing projects..."
    PROJECTS_RESP=$(curl -s "${WEBSITE_URL}/api/projects" \
        -H "Authorization: Bearer ${JWT}")
    PROJECT_ID=$(echo "$PROJECTS_RESP" | jq -r --arg name "$PROJECT_NAME" \
        '[.[] | select(.name == $name)] | first | .id // empty')
    if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "null" ]; then
        # Just use the first project
        PROJECT_ID=$(echo "$PROJECTS_RESP" | jq -r 'if type == "array" then .[0].id else .id end // empty')
    fi
fi

if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "null" ]; then
    error "Could not create or find a project. Response: $PROJECT_RESP"
    exit 1
fi
success "Project ID: ${PROJECT_ID}"

# ─── Get project API key ──────────────────────────────────────────────────────

info "Fetching project API key..."
KEYS_RESP=$(curl -s "${WEBSITE_URL}/api/projects/${PROJECT_ID}/keys" \
    -H "Authorization: Bearer ${JWT}")

PROJECT_API_KEY=$(echo "$KEYS_RESP" | jq -r 'if type == "array" then .[0].key else .key end // empty')

if [ -z "$PROJECT_API_KEY" ] || [ "$PROJECT_API_KEY" = "null" ]; then
    # Try creating a new key
    info "No key found; creating a new project API key..."
    KEY_RESP=$(curl -s -X POST "${WEBSITE_URL}/api/projects/${PROJECT_ID}/keys" \
        -H "Authorization: Bearer ${JWT}")
    PROJECT_API_KEY=$(echo "$KEY_RESP" | jq -r '.key // empty')
fi

if [ -z "$PROJECT_API_KEY" ] || [ "$PROJECT_API_KEY" = "null" ]; then
    error "Could not retrieve a project API key."
    exit 1
fi
success "Project API key: ${PROJECT_API_KEY}"

# ─── Configure LLM provider ───────────────────────────────────────────────────

if [ -n "${OPENAI_API_KEY:-}" ]; then
    info "Configuring OpenAI provider..."
    INT_RESP=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${WEBSITE_URL}/api/projects/${PROJECT_ID}/llm/integrations" \
        -H "Authorization: Bearer ${JWT}" \
        -H "Content-Type: application/json" \
        -d "{\"provider\":\"openai\",\"api_key\":\"${OPENAI_API_KEY}\",\"enabled\":true}")
    if [ "$INT_RESP" = "200" ] || [ "$INT_RESP" = "201" ]; then
        success "OpenAI provider configured."
    else
        warn "Integration endpoint returned HTTP ${INT_RESP}. You may need to configure it via the UI."
    fi
elif [ -n "${OLLAMA:-}" ]; then
    info "Configuring Ollama (OpenAI-compatible) provider with placeholder key..."
    INT_RESP=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${WEBSITE_URL}/api/projects/${PROJECT_ID}/llm/integrations" \
        -H "Authorization: Bearer ${JWT}" \
        -H "Content-Type: application/json" \
        -d '{"provider":"openai","api_key":"ollama","enabled":true}')
    if [ "$INT_RESP" = "200" ] || [ "$INT_RESP" = "201" ]; then
        success "Ollama provider configured (api_key=ollama placeholder)."
    else
        warn "Integration endpoint returned HTTP ${INT_RESP}. Try configuring manually via the UI."
    fi
else
    warn "No LLM provider configured. Set OPENAI_API_KEY=sk-... to auto-configure OpenAI,"
    warn "or set OLLAMA=1 if using 'make dev-ollama'. You can also configure it via the UI."
fi

# ─── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Dev environment seeded successfully!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${CYAN}Project ID:${NC}      ${PROJECT_ID}"
echo -e "  ${CYAN}Project API Key:${NC} ${PROJECT_API_KEY}"
echo ""
echo -e "  ${CYAN}Test the gateway:${NC}"
echo ""

if [ -n "${OLLAMA:-}" ]; then
    MODEL="openai/llama3.2"
else
    MODEL="openai/gpt-4o-mini"
fi

echo '  curl -s http://localhost:3003/api/gateway/v1/chat/completions \'
echo "    -H \"Authorization: Bearer ${PROJECT_API_KEY}\" \\"
echo '    -H "Content-Type: application/json" \'
printf '    -d '"'"'{"model":"%s","messages":[{"role":"user","content":"Say hello in one sentence"}]}'"'"' | jq .\n' "$MODEL"
echo ""
echo -e "  ${CYAN}List supported models:${NC}"
echo "  curl -s http://localhost:3003/api/gateway/v1/models \\"
echo "    -H \"Authorization: Bearer ${PROJECT_API_KEY}\" | jq ."
echo ""
echo -e "  ${CYAN}UI:${NC} http://localhost:5173"
echo ""
