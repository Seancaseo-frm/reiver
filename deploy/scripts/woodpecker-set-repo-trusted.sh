#!/usr/bin/env bash
# Set the Woodpecker repo to Trusted via API (fixes "Insufficient trust level" linter).
# Requires: you are server admin (in WOODPECKER_ADMIN) and have a Woodpecker PAT.
# Create a PAT: Woodpecker UI → click your avatar (top right) → copy token.
set -e
WOODPECKER_URL="${WOODPECKER_URL:-http://YOUR_SERVER_IP:31473}"
WOODPECKER_TOKEN="${WOODPECKER_TOKEN:-}"
REPO_FULL_NAME="${REPO_FULL_NAME:-}"

if [[ -z "$WOODPECKER_TOKEN" ]]; then
  echo "Usage: WOODPECKER_TOKEN=<your-woodpecker-pat> ./woodpecker-set-repo-trusted.sh"
  echo "Optional: WOODPECKER_URL=http://your-server:31473 REPO_FULL_NAME=owner/repo"
  echo "If REPO_FULL_NAME is empty, script infers from git remote (e.g. your-org/reiver)."
  exit 1
fi

if [[ -z "$REPO_FULL_NAME" ]]; then
  ORIGIN="$(git remote get-url origin 2>/dev/null || true)"
  if [[ "$ORIGIN" =~ github\.com[:/]([^/]+)/([^/.]+) ]]; then
    REPO_FULL_NAME="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
  else
    echo "Set REPO_FULL_NAME (e.g. your-org/reiver)"
    exit 1
  fi
fi

# Lookup repo by full name (slug format: owner/repo). Slash must be URL-encoded in path.
SLUG_ENC=$(echo "$REPO_FULL_NAME" | sed 's|/|%2F|g')
echo "Looking up repo: $REPO_FULL_NAME"
REPO_JSON=$(curl -sS -H "Authorization: Bearer $WOODPECKER_TOKEN" "${WOODPECKER_URL}/api/repos/lookup/${SLUG_ENC}" || true)
if [[ -z "$REPO_JSON" || "$REPO_JSON" == *"error"* || "$REPO_JSON" == *"404"* ]]; then
  echo "Failed to lookup repo. Ensure repo is activated in Woodpecker and WOODPECKER_TOKEN is an admin user's PAT."
  echo "Response: $REPO_JSON"
  exit 1
fi

REPO_ID=$(echo "$REPO_JSON" | sed -n 's/.*"id": *\([0-9]*\).*/\1/p' | head -1)
if [[ -z "$REPO_ID" ]]; then
  echo "Could not parse repo id from: $REPO_JSON"
  exit 1
fi

echo "Patching repo id $REPO_ID to trusted (security=true)..."
RESP=$(curl -sS -X PATCH \
  -H "Authorization: Bearer $WOODPECKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"trusted":{"security":true,"network":true,"volumes":true}}' \
  "${WOODPECKER_URL}/api/repos/${REPO_ID}")
if [[ "$RESP" == *"error"* ]] || [[ -z "$RESP" ]]; then
  echo "PATCH failed: $RESP"
  exit 1
fi
echo "Done. Repo is now trusted; the 'Insufficient trust level' linter errors should disappear."
