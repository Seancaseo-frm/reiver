# MinIO Test Data

This directory contains Parquet files for testing S3/R2 object storage federated queries.

## Events Table

The `events.parquet` file contains 5000 event records with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Unique event identifier |
| order_id | INT | Join key to MySQL orders.id |
| event_type | STRING | Type of event (order_created, payment_received, etc.) |
| event_data | STRING | JSON payload with event details |
| source_system | STRING | System that generated the event |
| timestamp | TIMESTAMP | When the event occurred |

## Generating the Parquet File

Run the Python script to generate test data:

```bash
cd scripts/seed/minio
python3 generate_events.py
```

Or use the convenience script:

```bash
./scripts/seed/minio/generate.sh
```

## Join Keys

- `order_id` references `orders.id` from MySQL
- Can be used for cross-database federated queries
