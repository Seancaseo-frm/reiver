# Pond — Data Warehouse

Pond is Reiver's federated data warehouse. Connect your databases, SaaS tools, files, and streaming sources, then query them all with standard SQL through any PostgreSQL client. No ETL pipelines required.

::: info
Detailed documentation for Pond is coming soon. This page provides a feature overview.
:::

## Features

### Federated SQL
Query across all connected data sources using standard SQL. Pond's federation planner optimizes cross-source joins automatically.

### PostgreSQL Wire Protocol
Connect with any PostgreSQL client — psql, Metabase, Grafana, Tableau, DBeaver, or your application's existing database driver.

```bash
psql -h reiver.ai -p 5433 -U dh_your_key
```

```sql
SELECT c.amount, o.status
FROM stripe.charges c
JOIN postgres.orders o ON c.metadata->>'order_id' = o.id::text
WHERE c.created > '2026-01-01';
```

### Data Sources

**Databases**
- PostgreSQL, MySQL, ClickHouse, SQL Server, MongoDB
- Snowflake, BigQuery, Redshift, SQLite

**SaaS**
- Stripe, Google Sheets

**Files**
- CSV, Excel, JSON / NDJSON

**Object Storage**
- S3, Cloudflare R2 (Parquet files)

**Streaming**
- Kafka, AWS Kinesis

**Blockchain**
- Ethereum, Solana, Bitcoin, Polygon

### Storage Tiers
- **Hot** — ClickHouse-backed for frequently queried data with sub-second latency.
- **Warm** — R2/S3 Parquet files with skip indexes for cost-efficient analytical queries.
- **Cold** — On-demand federation for infrequently accessed sources.

### Skip Index System
Parquet files are indexed with FST, Xor filters, and MinMax statistics for efficient file pruning. Only relevant files are scanned during queries.

### Catalog Service
Automatic schema discovery and lineage tracking across all connected sources. See column-level dependencies and data flow.

### Sync Pipeline
Configurable sync schedules with interval, cron, and manual triggers. Track sync status, row counts, and error rates.

### Natural Language Queries
Ask questions in plain English and Pond translates them to SQL across your connected sources.
