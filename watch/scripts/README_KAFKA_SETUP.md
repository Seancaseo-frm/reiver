# Kafka Development Setup

## Quick Start

Kafka is now included in `docker-compose.yml`. To start all services including Kafka:

```bash
# Start all services
docker-compose up -d

# Or start just Kafka dependencies
docker-compose up -d zookeeper kafka
```

## Services

- **Zookeeper**: Required by Kafka (runs on port 2181)
- **Kafka**: Kafka broker (runs on port 9092)

## Configuration

The Kafka service is configured with:
- Auto-create topics enabled (topics are created automatically when needed)
- 7-day log retention
- 1GB log segment size
- Single broker (sufficient for development)

## Creating Topics

While auto-creation is enabled, you can manually create topics using the script:

```bash
# Make sure Kafka is running
docker-compose up -d kafka

# Create topics
./scripts/create_kafka_topics.sh
```

Or manually:

```bash
docker-compose exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic reiver.exceptions \
  --partitions 3 \
  --replication-factor 1

docker-compose exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic reiver.spans \
  --partitions 3 \
  --replication-factor 1
```

## Environment Variables

When running the API locally (not in Docker), set:

```bash
export KAFKA_HOSTS=localhost:9092
```

When running in Docker, Kafka is automatically available at `kafka:9093` (internal network).

## Verifying Kafka is Working

```bash
# Check Kafka is running
docker-compose ps kafka

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic
docker-compose exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic reiver.exceptions

# View messages (consumer)
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic reiver.exceptions \
  --from-beginning
```

## Troubleshooting

### Kafka won't start
- Make sure Zookeeper is running first: `docker-compose up -d zookeeper`
- Check logs: `docker-compose logs kafka`

### Can't connect to Kafka
- Verify Kafka is healthy: `docker-compose ps`
- Check if port 9092 is available: `lsof -i :9092`
- For local development, use `localhost:9092`
- For Docker services, use `kafka:9093` (internal network)

### Topics not created
- Topics are auto-created when first message is sent
- Or manually create them using the script above

## Development Workflow

1. Start all services: `docker-compose up -d`
2. Wait for services to be healthy: `docker-compose ps`
3. Start your API: `cargo run`
4. The API will automatically connect to Kafka and create topics when needed

## Port Reference

- **Zookeeper**: 2181
- **Kafka**: 9092 (external), 9093 (internal Docker network)


