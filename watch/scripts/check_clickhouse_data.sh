#!/usr/bin/env bash
#
# Query ClickHouse to verify observability data is being received.
# Requires: ClickHouse HTTP on localhost:8123 (or set CLICKHOUSE_URL).
#
# Usage: ./scripts/check_clickhouse_data.sh [project_id]
#   If project_id is given, filters counts by that project.
#

set -e

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
PROJECT_ID="${1:-}"

query() {
  curl -s "$CLICKHOUSE_URL/" --data-binary "$1"
}

echo "📊 ClickHouse data check (database: reiver)"
echo "   URL: $CLICKHOUSE_URL"
echo ""

# Build optional project filter
if [ -n "$PROJECT_ID" ]; then
  PF="WHERE project_id = '$PROJECT_ID'"
  echo "   Project filter: $PROJECT_ID"
  echo ""
else
  PF=""
fi

echo "=== Row counts ==="
query "
  SELECT 'exceptions'       AS table_name, count() AS rows FROM reiver.exceptions $PF
  UNION ALL SELECT 'spans', count() FROM reiver.spans $PF
  UNION ALL SELECT 'metrics (legacy)', count() FROM reiver.metrics $PF
  UNION ALL SELECT 'samples_v1 (metrics)', count() FROM reiver.samples_v1 $PF
  UNION ALL SELECT 'logs', count() FROM reiver.logs $PF
  UNION ALL SELECT 'unstructured_logs', count() FROM reiver.unstructured_logs $PF
  FORMAT PrettyCompact
"
echo ""

echo "=== Time range (spans) ==="
query "SELECT project_id, min(start_time) AS min_ts, max(start_time) AS max_ts, count() AS cnt FROM reiver.spans $PF GROUP BY project_id FORMAT PrettyCompact"
echo ""

echo "=== Time range (metrics: samples_v1) ==="
query "SELECT project_id, min(toDateTime(intDiv(unix_milli,1000))) AS min_ts, max(toDateTime(intDiv(unix_milli,1000))) AS max_ts, count() AS cnt FROM reiver.samples_v1 $PF GROUP BY project_id FORMAT PrettyCompact"
echo ""

echo "=== Sample: recent spans (project, service, operation, start_time) ==="
query "SELECT project_id, service_name, operation_name, start_time FROM reiver.spans $PF ORDER BY start_time DESC LIMIT 5 FORMAT PrettyCompact"
