#!/bin/bash
# Script to update ClickHouse Kafka Engine broker address
# Usage: ./scripts/update_clickhouse_kafka_broker.sh <new_broker_address>
# Example: ./scripts/update_clickhouse_kafka_broker.sh localhost:9092

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <new_broker_address>"
    echo "Example: $0 localhost:9092"
    exit 1
fi

NEW_BROKER="$1"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://default:@localhost:8123}"

echo "⚠️  WARNING: This will DROP and recreate ClickHouse Kafka Engine tables!"
echo "This will stop exception ingestion temporarily until the tables are recreated."
echo "New broker address: $NEW_BROKER"
echo ""
read -p "Are you sure you want to continue? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Dropping existing Kafka Engine tables..."
docker-compose exec -T clickhouse clickhouse-client --query "DROP TABLE IF EXISTS reiver.exceptions_mv" || true
docker-compose exec -T clickhouse clickhouse-client --query "DROP TABLE IF EXISTS reiver.exceptions_kafka" || true

echo "Creating Kafka Engine table with new broker address: $NEW_BROKER"
docker-compose exec -T clickhouse clickhouse-client --query "
CREATE TABLE reiver.exceptions_kafka (
    value String
) ENGINE = Kafka('$NEW_BROKER', 'reiver.exceptions', 'clickhouse-exceptions-consumer-group', 'JSONEachRow')
SETTINGS
    kafka_poll_max_batch_size = 100000,
    kafka_flush_interval_ms = 7500,
    kafka_max_block_size = 1000000,
    kafka_skip_broken_messages = 1
"

echo "Creating Materialized View..."
docker-compose exec -T clickhouse clickhouse-client --query "
CREATE MATERIALIZED VIEW reiver.exceptions_mv TO reiver.exceptions AS
SELECT
    JSONExtractString(value, 'id') AS id,
    JSONExtractString(value, 'project_id') AS project_id,
    JSONExtractString(value, 'fingerprint') AS fingerprint,
    JSONExtractString(value, 'level') AS level,
    JSONExtractString(value, 'message') AS message,
    nullIf(JSONExtractString(value, 'exception_type'), '') AS exception_type,
    nullIf(JSONExtractString(value, 'exception_value'), '') AS exception_value,
    JSONExtractString(value, 'stacktrace') AS stacktrace,
    JSONExtractString(value, 'context') AS context,
    JSONExtractString(value, 'tags') AS tags,
    JSONExtractString(value, 'user_data') AS user_data,
    parseDateTime64BestEffortOrNull(JSONExtractString(value, 'timestamp')) AS timestamp,
    now64() AS created_at
FROM reiver.exceptions_kafka
WHERE JSONHas(value, 'id')
"

echo "✅ Kafka Engine tables recreated successfully with broker: $NEW_BROKER"
