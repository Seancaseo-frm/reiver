#!/usr/bin/env bash
set -euo pipefail

require_status() {
  local url="$1"
  local expected="${2:-200}"
  local status
  status="$(curl --location --silent --show-error --output /dev/null --write-out '%{http_code}' "$url")"
  if [[ "$status" != "$expected" ]]; then
    echo "FAIL $url returned $status (expected $expected)" >&2
    return 1
  fi
  echo "PASS $url returned $status"
}

require_content() {
  local url="$1"
  local expected="$2"
  local body
  body="$(curl --location --fail --silent --show-error "$url")"
  if [[ "$body" != *"$expected"* ]]; then
    echo "FAIL $url does not contain the expected onboarding marker" >&2
    return 1
  fi
  echo "PASS $url contains the expected onboarding marker"
}

require_status "https://reiver.ai/quickstart"
require_status "https://reiver.ai/llms.txt"
require_status "https://reiver.ai/llms-full.txt"
require_status "https://docs.reiver.ai/quickstart"
require_status "https://docs.reiver.ai/robots.txt"
require_status "https://docs.reiver.ai/llms.txt"
require_status "https://docs.reiver.ai/llms-full.txt"
require_status "https://docs.reiver.ai/flow/session-telemetry"
require_content "https://docs.reiver.ai/quickstart" "choose your Reiver path"
require_content "https://docs.reiver.ai/quickstart" "REIVER_FLOW_API_KEY"
require_content "https://docs.reiver.ai/flow/session-telemetry" "Session and Identity Contract"
require_content "https://docs.reiver.ai/robots.txt" "ClaudeBot"
require_content "https://reiver.ai/llms.txt" "agent://onboarding"
require_content "https://reiver.ai/llms.txt" "independently completable"
require_content "https://reiver.ai/llms-full.txt" "Delegated autonomy"
require_content "https://reiver.ai/llms-full.txt" "Select the onboarding track"
require_content "https://docs.reiver.ai/llms.txt" "agent://onboarding"
require_content "https://docs.reiver.ai/llms-full.txt" "Business-aware activation"
require_content "https://docs.reiver.ai/llms-full.txt" "Session and Identity Contract"

unauthenticated_mcp_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --request POST \
  --header "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","id":1,"method":"resources/list","params":{}}' \
  "https://reiver.ai/mcp")"
if [[ "$unauthenticated_mcp_status" != "401" ]]; then
  echo "FAIL unauthenticated MCP resource read returned ${unauthenticated_mcp_status} (expected 401)" >&2
  exit 1
fi
echo "PASS unauthenticated MCP resource read returned 401"

if [[ -n "${REIVER_AGENT_TOKEN:-}" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    echo "FAIL jq is required for the optional MCP resource check" >&2
    exit 1
  fi

  mcp_response="$(curl --fail --silent --show-error \
    --request POST \
    --header "Authorization: Bearer ${REIVER_AGENT_TOKEN}" \
    --header "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"agent://onboarding"}}' \
    "https://reiver.ai/mcp")"

  if ! jq --exit-status \
    '.result.contents[0].text
      | contains("Reiver application onboarding contract")
        and contains("traces")
        and contains("logs")
        and contains("metrics")
        and contains("business outcome")
        and contains("hard technical boundary")
        and contains("Select the onboarding track")
        and contains("Session and Identity Contract")
        and contains("synthetic sessions")' \
    >/dev/null <<<"$mcp_response"; then
    echo "FAIL MCP did not return the complete agent://onboarding resource" >&2
    exit 1
  fi
  echo "PASS MCP returned the complete agent://onboarding resource"
else
  echo "SKIP MCP resource check (REIVER_AGENT_TOKEN is not set)"
fi

echo "Public onboarding surfaces passed. Run the credentialed Flow and Watch acceptance checks separately."
