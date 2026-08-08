#!/bin/bash

# Script to create Kafka topics for Reiver
# Usage: ./scripts/create_kafka_topics.sh [kafka-broker]
# Example: ./scripts/create_kafka_topics.sh localhost:9092
#          ./scripts/create_kafka_topics.sh  # Uses docker-compose exec if available

# Check if running in Docker Compose environment
# Support both Redpanda (using rpk) and Kafka (using kafka-topics)
if command -v docker-compose &> /dev/null && docker-compose ps redpanda &> /dev/null; then
    echo "Detected Docker Compose environment with Redpanda, using rpk..."
    KAFKA_BROKER="${1:-localhost:9092}"
    # Use rpk for Redpanda (Kafka-compatible)
    KAFKA_BIN="docker-compose exec -T redpanda rpk topic"
elif command -v docker-compose &> /dev/null && docker-compose ps kafka &> /dev/null; then
    echo "Detected Docker Compose environment with Kafka, using kafka-topics..."
    KAFKA_BROKER="${1:-localhost:9092}"
    KAFKA_BIN="docker-compose exec -T kafka kafka-topics --bootstrap-server"
else
    KAFKA_BROKER="${1:-localhost:9092}"
    KAFKA_BIN="${KAFKA_BIN:-kafka-topics.sh}"
fi

echo "Creating Kafka topics for Reiver..."
echo "Kafka broker: $KAFKA_BROKER"

# Check if using Redpanda (rpk) or Kafka (kafka-topics)
USE_RPK=false
if command -v docker-compose &> /dev/null && docker-compose ps redpanda &> /dev/null; then
    USE_RPK=true
fi

# Create reiver.exceptions topic
echo ""
echo "Creating topic: reiver.exceptions"
if [ "$USE_RPK" = true ]; then
    # Redpanda uses rpk topic create (replication is handled internally via Raft)
    docker-compose exec -T redpanda rpk topic create reiver.exceptions \
      --partitions 3 \
      --retention-time 7d \
      --compression-type snappy || {
        echo "Failed to create reiver.exceptions topic (might already exist)"
    }
else
    $KAFKA_BIN --create \
      --bootstrap-server "$KAFKA_BROKER" \
      --topic reiver.exceptions \
      --partitions 3 \
      --replication-factor 1 \
      --if-not-exists \
      --config retention.ms=604800000 \
      --config compression.type=snappy || {
        echo "Failed to create reiver.exceptions topic"
        exit 1
    }
fi

# Create reiver.spans topic
echo ""
echo "Creating topic: reiver.spans"
if [ "$USE_RPK" = true ]; then
    # Redpanda uses rpk topic create (replication is handled internally via Raft)
    docker-compose exec -T redpanda rpk topic create reiver.spans \
      --partitions 3 \
      --retention-time 7d \
      --compression-type snappy || {
        echo "Failed to create reiver.spans topic (might already exist)"
    }
else
    $KAFKA_BIN --create \
      --bootstrap-server "$KAFKA_BROKER" \
      --topic reiver.spans \
      --partitions 3 \
      --replication-factor 1 \
      --if-not-exists \
      --config retention.ms=604800000 \
      --config compression.type=snappy || {
        echo "Failed to create reiver.spans topic"
        exit 1
    }
fi

# Create reiver.pipeline.events topic
echo ""
echo "Creating topic: reiver.pipeline.events"
if [ "$USE_RPK" = true ]; then
    docker-compose exec -T redpanda rpk topic create reiver.pipeline.events \
      --partitions 3 \
      --retention-time 7d \
      --compression-type snappy || {
        echo "Failed to create reiver.pipeline.events topic (might already exist)"
    }
else
    $KAFKA_BIN --create \
      --bootstrap-server "$KAFKA_BROKER" \
      --topic reiver.pipeline.events \
      --partitions 3 \
      --replication-factor 1 \
      --if-not-exists \
      --config retention.ms=604800000 \
      --config compression.type=snappy || {
        echo "Failed to create reiver.pipeline.events topic"
        exit 1
    }
fi

echo ""
echo "✅ Kafka topics created successfully!"
echo ""
echo "Topics created:"
echo "  - reiver.exceptions (3 partitions, 1 replication factor)"
echo "  - reiver.spans (3 partitions, 1 replication factor)"
echo "  - reiver.pipeline.events (3 partitions, 1 replication factor)"
echo ""
echo "Configuration:"
echo "  - Retention: 7 days (604800000 ms)"
echo "  - Compression: snappy"
echo ""
echo "To verify, run:"
echo "  $KAFKA_BIN --list --bootstrap-server $KAFKA_BROKER"

