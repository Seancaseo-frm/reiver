# Reiver - High-Performance Observability Platform

**Full-stack APM with integrated AI observability. Traces, logs, metrics, and LLM monitoring in one platform.**

## Products

- **Watch** — Application Performance Monitoring (APM)
- **Flow** — LLM Gateway
- **Pond** — Data Warehouse

Built with Rust for 10x faster performance and 50% lower cost than legacy platforms.

## Features

- ⚡ **Fast** - Built in Rust for 10x better performance
- 🔒 **Secure** - JWT authentication, rate limiting, secure API keys
- 📊 **Dashboard** - Beautiful Vue.js dashboard for full observability
- 🔔 **Alerts** - Slack, PagerDuty, Discord, Teams, ServiceNow notifications
- 🐳 **Self-hostable** - One-command Docker setup
- 🐍 **Python SDK** - Easy integration for exception tracking
- 📈 **Error Grouping** - Intelligent fingerprinting groups similar errors
- 🤖 **AI Gateway** - Unified API for OpenAI, Anthropic, Google Gemini, AWS Bedrock
- 📊 **LLM Observability** - Token usage, costs, latency tracking for all LLM calls

## Two Products, One Platform

### Reiver APM

Full-stack application performance monitoring:

- **Distributed Tracing**: OTLP-native, end-to-end request visualization
- **Error Tracking**: Automatic exception grouping, smart fingerprinting
- **Log Aggregation**: Structured logging with semantic search
- **Real-Time Metrics**: Custom dashboards with threshold and anomaly alerting
- **Database Monitoring**: Query explain plans, slow query detection
- **Continuous Profiling**: CPU and memory flamegraphs linked to traces
- **Synthetic Monitoring**: HTTP, TCP, SSL health checks from multiple locations
- **Cloud Integrations**: Native AWS, Azure, GCP, Oracle Cloud monitoring

### Reiver Gateway

Unified LLM API with built-in observability:

- **One API, All Providers**: OpenAI, Anthropic, Google Gemini, AWS Bedrock
- **Real-Time Streaming**: SSE streaming for all providers
- **Automatic Failover**: Retry logic with cross-provider fallback
- **Semantic Caching**: Reduce costs with intelligent response caching
- **Prompt Management**: Version control and A/B testing for prompts
- **Cost Analytics**: Per-request token usage and spend tracking

## Market Position

| Platform | Full APM | LLM Monitoring | LLM Gateway |
|----------|----------|----------------|-------------|
| Datadog | Yes | Yes | No |
| New Relic | Yes | Yes | No |
| Portkey | No | Yes | Yes |
| Helicone | No | Yes | Yes |
| **Reiver** | **Yes** | **Yes** | **Yes** |

### Why This Matters

Most AI applications are 80% traditional code and 20% LLM calls. When something goes wrong, you need to see the complete picture:

1. API request comes in (200ms)
2. Database query fetches context (150ms slow query)
3. LLM call to OpenAI (2.5s, 1,500 tokens, $0.03)
4. Response returned (50ms)

**With Reiver**: See all 4 steps in one trace  
**With LLM-only tools**: Only see step 3  
**With traditional APM**: Need separate SDK integration for LLM visibility

## Quick Start - AI Gateway

Use any LLM provider through a unified API. **No special SDK required** - just change the base URL in your existing OpenAI SDK:

```python
from openai import OpenAI

client = OpenAI(
    api_key="dh_your_project_key",
    base_url="https://reiver.ai/api/gateway/v1"  # Point to Reiver gateway
)

# Use ANY model - gateway routes automatically
response = client.chat.completions.create(
    model="claude-3-opus",  # or gpt-4o, gemini-pro, etc.
    messages=[{"role": "user", "content": "Hello!"}]
)
```

Supported models:
- **OpenAI**: gpt-4o, gpt-4-turbo, gpt-3.5-turbo, o1-preview, o1-mini
- **Anthropic**: claude-3-opus, claude-3-sonnet, claude-3-haiku, claude-3.5-sonnet
- **Google**: gemini-pro, gemini-1.5-pro, gemini-1.5-flash, gemini-2.0-flash
- **AWS Bedrock**: All Bedrock models (Claude, Titan, Llama, Mistral)

## Quick Start - Error Tracking

### Python SDK

```bash
pip install reiver
```

```python
import reiver

reiver.init(
    api_key="dh_your_project_key",
    api_url="https://api.reiver.io",
    environment="production"
)

try:
    risky_operation()
except Exception as e:
    reiver.capture_exception(e)
```

### Rust SDK

```toml
[dependencies]
reiver-sdk = "0.1"
```

```rust
let _guard = reiver::init(reiver::ClientOptions {
    api_key: "dh_your_project_key".to_string(),
    environment: Some("production".to_string()),
    ..Default::default()
});

if let Err(e) = some_operation() {
    reiver::capture_exception(&e);
}
```

## Self-Hosting

### Prerequisites

- Docker and Docker Compose
- Rust (for local development)
- Node.js 18+ (for frontend development)

### Running with Docker

1. Clone the repository:
```bash
git clone https://github.com/reiver/reiver.git
cd reiver
```

2. Set environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Start services:
```bash
docker-compose up -d
```

The API will be available at `http://localhost:3000` and the frontend at `http://localhost:3001`.

### Local Development

Reiver supports two local development modes:

| Mode | Command | Use Case |
|------|---------|----------|
| **Dev Mode** | `make dev` | Daily development, easy debugging |
| **Production-Like Mode** | `make prod-local-up` | Test production deployments locally |

#### Mode 1: Dev Mode (Recommended for Development)

Databases run in Docker, app runs on host with `cargo run`. Best for fast iteration and debugging.

**Prerequisites:**

- **Docker & Docker Compose** - for running databases
- **Rust** (latest stable) - for the backend
- **Node.js 18+** - for the frontend
- **sqlx-cli** - for PostgreSQL migrations: `cargo install sqlx-cli`
- **xmlsec dependencies** - for SAML authentication (see below)

**Quick Start:**

```bash
# First time setup (builds images, verifies Keeper)
make setup

# Start development (databases + frontend + server)
make dev
```

This command:
1. Starts PostgreSQL, Redis, ClickHouse (with Keeper), and Redpanda
2. Creates Kafka topics
3. Builds the frontend
4. Runs database migrations automatically
5. Starts the API server

The API is available at `http://localhost:3000`.

#### Available Make Commands

```bash
make help              # Show all commands
make dev               # Start full dev environment
make dev-api           # Run API only (no workers)
make dev-workers       # Run workers only (no API)
make dev-split         # Run API and workers as separate processes

make reset-db          # Drop and recreate databases (WARNING: deletes data)
make wipe              # Clear data but keep table structure
make migrate           # Run PostgreSQL migrations only
```

### Environment Variables

```bash
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reiver
export REDIS_URL=redis://localhost:6379
export CLICKHOUSE_URL=http://default:@localhost:8123
export KAFKA_HOSTS=localhost:19092
export JWT_SECRET=dev-secret-change-in-production
```

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                          Clients                                    │
├──────────────────────┬─────────────────────────────────────────────┤
│   SDKs (Python/Rust) │      OpenAI SDK (for Gateway)               │
└──────────┬───────────┴───────────────────────┬─────────────────────┘
           │                                   │
           ▼                                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Reiver API (Rust/Axum)                     │
├──────────────┬─────────────┬──────────────┬─────────────────────┤
│  APM APIs    │ Gateway APIs │ Auth/SAML    │   Dashboard APIs    │
└──────────────┴─────────────┴──────────────┴─────────────────────┘
           │                   │
           ▼                   ▼
┌──────────────────┐  ┌─────────────────────┐
│   PostgreSQL     │  │     ClickHouse      │
│   (Auth, Config) │  │  (Telemetry Data)   │
└──────────────────┘  └─────────────────────┘
           │                   │
           ▼                   ▼
┌──────────────────┐  ┌─────────────────────┐
│     Redis        │  │     Redpanda        │
│ (Cache, Queues)  │  │  (Event Streaming)  │
└──────────────────┘  └─────────────────────┘
```

## Data Warehouse Architecture

todo: datafusion allows loading of a few different formats into Arrow (e.g. CSV)
todo: if the customer isnt technical and can't specify the nature of the data in the config, so we can index the best way, we can make an AI to look at the customer data and generate their config

Reiver includes a data warehouse feature that allows users to query data from external sources (Stripe, PostgreSQL, etc.) using SQL. This section documents our architectural decisions.

### Why This Matters: The Data Silo Problem

Most businesses have data scattered across disconnected systems - CSV exports from marketing tools, a PostgreSQL production database, analytics in a separate warehouse, logs in S3. This fragmentation is costly:

- **$15 million per year**: Average cost to businesses from poor data quality (Gartner)
- **30% revenue loss**: Companies lose up to 30% annually due to inefficiencies from siloed data (IDC)
- **Only 28% connected**: Despite recognizing the problem, only 28% of enterprise applications are integrated
- **53% inconsistent**: Over half of business leaders report data inconsistencies across their tools

The data integration market is projected to grow from **$15.2 billion to $47.6 billion by 2034**, reflecting the urgent demand for solutions.

### The AI Efficiency Crisis

Despite massive investments, most businesses are failing to get value from AI:

- **95% of GenAI pilots deliver zero ROI** despite $30-40B invested (MIT, 2025)
- **74% of companies show no tangible value** from their AI initiatives
- **Only 5% of organizations** successfully translate AI pilots into operational impact
- **75% of AI initiatives fail** to deliver expected returns over three years (IBM)
- **2-4 years** typical time to achieve ROI on an AI use case vs 7-12 months expected

**Why AI Projects Fail:**

| Problem | What Happens | Result |
|---------|--------------|--------|
| **No business context** | AI can only see one system at a time | Generic answers, not actionable insights |
| **Verification tax** | Employees double-check every AI output | Promised efficiency gains eliminated |
| **No learning** | AI doesn't adapt to your workflows | Same mistakes repeated |
| **Technology-first thinking** | Buy AI tools, hope for value | No clear use case, no ROI |

**The Root Cause**: AI tools aren't connected to your actual business data.

ChatGPT can answer general questions. But it can't tell you why revenue dropped last month, which customers are at risk of churning, or whether your slow API is costing you money - because it doesn't have access to your Stripe data, your PostgreSQL database, your support tickets, or your application metrics.

### How Reiver Solves the AI Efficiency Problem

Reiver provides a **clear strategy for integrating AI into your business**:

```
Step 1: Connect your data sources (5 minutes each)
        → Stripe, PostgreSQL, CSV files, your app's metrics

Step 2: AI can now see everything
        → Cross-reference payments, errors, usage, support tickets

Step 3: Ask business questions in plain English
        → "Why did enterprise churn increase?" 
        → "Which API endpoints lose us money?"
        → "What's causing checkout failures?"

Step 4: Get actionable answers with real data
        → Not generic advice, but specific insights from YOUR data
```

**Why this works when other AI approaches fail:**

| Other AI Tools | Reiver |
|----------------|-----------|
| See one data source | See all your data sources unified |
| Give generic answers | Give answers specific to your business |
| Require data engineering to connect | Connect in minutes, no pipelines |
| Can't verify answers | Shows the actual data behind every insight |
| Don't improve over time | Learns your business context |

**The 5% that succeed with AI** share one trait: they integrated AI with their core business data. Reiver makes this integration trivial instead of a multi-year infrastructure project.

### How We Differ from Snowflake and Databricks

Traditional data warehouses like Snowflake and Databricks require you to **ETL (Extract, Transform, Load)** all your data into their platform before you can query it. This means building and maintaining complex data pipelines.

| Aspect | Snowflake / Databricks | Reiver |
|--------|------------------------|-----------|
| **Setup time** | Weeks to months (build ETL pipelines) | Minutes (connect and query) |
| **Data movement** | Copy everything to warehouse | Query data where it lives |
| **Data freshness** | Stale (batch ETL updates) | Real-time (query source directly) |
| **Storage cost** | Pay for duplicated data | No duplication |
| **Typical cost** | $100K-$1M+/year | Fraction of the cost |

**Example**: To analyze Stripe payments alongside your PostgreSQL orders and CSV exports:

- **Traditional approach**: Build a Fivetran pipeline for Stripe, another for Postgres, write a script to upload CSVs, wait for nightly sync, then query in Snowflake
- **Reiver approach**: Connect all three sources, immediately run:
  ```sql
  SELECT * FROM stripe.charges 
  JOIN postgres.orders ON charges.order_id = orders.id
  JOIN csv.marketing_campaigns ON orders.campaign_id = campaigns.id
  ```

**Trade-offs**: Traditional warehouses excel at complex analytics on massive pre-aggregated datasets where query speed on billions of rows is critical. Reiver is ideal for:

- Joining data across sources without moving it
- Real-time queries on operational data  
- Teams without dedicated data engineering resources
- Cost-conscious organizations
- Compliance scenarios where data must stay in customer-controlled storage

### Supported and Planned Data Sources

**Currently Supported:**

| Category | Sources | Notes |
|----------|---------|-------|
| **Databases** | PostgreSQL, ClickHouse, MySQL | Full schema discovery and sync |
| **File Formats** | Parquet, Delta Lake, Apache Iceberg | Native table format support |
| **Files** | CSV, JSON/NDJSON, Excel | Schema inference, local/S3/HTTP sources |
| **Object Storage** | S3, Cloudflare R2 | FST skip index for fast queries |
| **SaaS** | Stripe | Full API sync with incremental updates |
| **Streaming** | Kafka, AWS Kinesis | Consumer with offset tracking |
| **Blockchain** | Ethereum, Solana, Bitcoin, Polygon | Block range queries, address filtering |

**Infrastructure Ready (types, capabilities, and base classes implemented):**

These connectors have full type definitions, predicate pushdown capability matrices, and base infrastructure. They need API-specific implementation to be production-ready.

| Category | Sources | What's Implemented |
|----------|---------|-------------------|
| **Databases** | MongoDB, SQL Server, SQLite, Snowflake, BigQuery, Redshift | Type mapping, capability matrix, external DB backend |
| **CRM & Sales** | Salesforce, HubSpot | OAuth config, SOQL/filter capabilities |
| **E-commerce** | Shopify, WooCommerce | API filter capabilities, GraphQL support planned |
| **Marketing** | Google Analytics, Facebook Ads, Google Ads | Date range filtering, GAQL support |
| **Support** | Zendesk, Intercom | Incremental sync capabilities |
| **Accounting** | QuickBooks, Xero | Date filtering, OAuth flow |
| **Product Analytics** | Mixpanel, Amplitude, PostHog | Property filtering, aggregation support |
| **Dev Tools** | GitHub, Jira, Linear | GraphQL/JQL filtering capabilities |
| **Productivity** | Notion, Confluence, Airtable, Asana, Monday.com | Property/formula filtering |
| **Spreadsheets** | Google Sheets | OAuth config ready |
| **Cloud Storage** | Google Cloud Storage, Azure Blob | Object storage backend ready |

**Planned Connectors:**

| Category | Sources | Why It Matters |
|----------|---------|----------------|
| **Files** | XML | Legacy enterprise data exports |
| **Databases** | Oracle, MariaDB, CockroachDB | Enterprise database coverage |
| **Data Platforms** | Databricks, Firebolt | Modern lakehouse integrations |
| **Messaging** | Slack, Microsoft Teams | Communication analytics |
| **HR** | Workday, BambooHR | People analytics |
| **Other SaaS** | Twilio, SendGrid, Mailchimp | Communication channel data |

**Example insights these connectors unlock:**

- "Which Notion docs are linked to features that drive the most revenue?"
- "Correlate Jira sprint velocity with deployment error rates and customer churn"
- "Which Asana projects are associated with churned customers?"
- "Connect Airtable CRM data with Stripe payments and support tickets"
- "Which GitHub PRs reduced errors AND increased conversion?"
- "What's the on-chain wallet activity of our highest-value customers?"

We prioritize connectors based on user demand. [Request a connector](https://github.com/reiver/reiver/issues).

### BI Tool Integrations (Planned)

Reiver can serve as a **data unification layer** for your existing BI tools. Since we use ClickHouse as our query engine, any BI tool with ClickHouse support can connect to Reiver and query all your unified data sources.

**Keep your existing BI tool. Expand what data it can access.**

| BI Tool | Type | Connector | Target Users |
|---------|------|-----------|--------------|
| **Looker** | Enterprise BI | Built-in JDBC | Enterprise, Google Cloud users |
| **Tableau** | Enterprise BI | JDBC connector (TACO) | Enterprise, analysts |
| **Power BI** | Enterprise BI | ODBC/Native connector | Microsoft shops |
| **Metabase** | Open Source | Plugin (JAR) | Startups, self-hosted teams |
| **Apache Superset** | Open Source | clickhouse-connect (pip) | Data engineers, technical teams |
| **Grafana** | Observability + BI | Native plugin | DevOps, SRE teams |
| **Redash** | Open Source | Built-in | Technical users |
| **Mode** | Cloud BI | JDBC | Analytics teams |
| **Sigma** | Cloud BI | JDBC | Business users |

**How it works:**

```
┌─────────────────────────────────────────────────────────────────────┐
│     Your Existing BI Tool (Tableau, Looker, Metabase, etc.)         │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ ClickHouse protocol
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Reiver (Data Unification Layer)               │
├─────────────────────────────────────────────────────────────────────┤
│  Stripe │ Postgres │ S3/Parquet │ Blockchain │ CSV │ Salesforce    │
└─────────────────────────────────────────────────────────────────────┘
```

**Why this matters:**

- Your BI tool can't query Stripe directly
- Tableau can't read blockchain transactions
- Power BI can't join PostgreSQL with S3 Parquet files
- Looker requires data to be ETL'd into a warehouse first

Reiver solves all of these - then your existing BI tool queries Reiver as if it were a regular database.

### AI Business Analyzer (Coming Soon)

Because Reiver can query across **all** your data sources simultaneously, we're building an AI-powered business analyzer that no other tool can match.

**The Problem with Current AI Analytics Tools:**

Most AI analytics tools can only see one data source at a time. When you ask "Why did revenue drop last month?", they can only look at your sales data - not your marketing spend, support tickets, product changes, or inventory levels.

**Reiver's Advantage:**

With unified access to every data source, our AI can answer questions that require cross-system analysis:

| Question | Data Sources Needed | Traditional Tools | Reiver AI |
|----------|---------------------|-------------------|--------------|
| "Why did revenue drop last month?" | Sales + Marketing + Support + Product | Need 4 separate analyses | Single answer |
| "Which marketing campaigns drive the highest LTV customers?" | Ads + CRM + Payments + Support | Manual correlation | Automatic |
| "What's causing increased support tickets?" | Support + Product logs + Orders | Impossible | Natural language query |
| "Predict churn risk for enterprise accounts" | CRM + Usage + Billing + Support | Requires data engineering | Ask and get answer |

**How It Works:**

```
User: "Why are enterprise customers churning more this quarter?"

Reiver AI:
1. Queries Salesforce for churned accounts
2. Joins with Stripe for payment history  
3. Correlates with Zendesk support tickets
4. Checks product usage from your PostgreSQL database
5. Returns: "Enterprise churn increased 23% due to:
   - 67% had 3+ unresolved support tickets before churning
   - Average response time for enterprise tickets was 48hrs (vs 12hrs target)
   - Recommendation: Prioritize enterprise support queue"
```

**Why This Matters:**

- **No other tool can do this** - competitors only see partial data
- **No SQL required** - business users ask questions in plain English
- **Real-time insights** - queries live data, not stale warehouse copies
- **Actionable answers** - not just charts, but recommendations

This is only possible because Reiver has already solved the hard problem: unified access to heterogeneous data sources.

### Competitive Landscape

#### Tier 1: Direct Competitors (Startups)

| Company | Approach | Differentiator |
|---------|----------|----------------|
| **Spice.ai** | SQL federation + local DuckDB/SQLite acceleration | Lightweight runtime, sub-second queries via local caching |
| **GlareDB** | Query Postgres, Snowflake, S3 Parquet, Excel in single SQL | Simple developer experience, open-source |
| **MotherDuck** | DuckDB in the cloud with hybrid local/cloud execution | 70-90% cost reduction vs Snowflake, "No-ETL" positioning |
| **Opteryx** | Python-native federated query engine | Serverless, time-travel queries |
| **MindsDB** | Federated queries + AI/ML integration | AI-first, supports AI agents querying data |

#### Tier 2: Enterprise Data Virtualization (Expensive)

| Company | Approach | Target Market |
|---------|----------|---------------|
| **Starburst** (Trino commercial) | Managed Trino with enterprise features | Enterprise, data mesh |
| **Denodo** | Data virtualization platform | Fortune 500, legacy integration |
| **Promethium** | Universal query engine + metadata discovery | Enterprise, zero data movement |
| **Timbr.ai** | Ontology-based semantic data virtualization | Enterprise, knowledge graphs |

#### Tier 3: Traditional Stack (ETL Required)

| Company | Approach | Why It's Slower |
|---------|----------|-----------------|
| **Fivetran + Snowflake** | ETL + warehouse bundle | Weeks to set up, stale data, expensive |
| **Airbyte + DuckDB** | Open source ETL + local analytics | Still requires ETL pipelines |
| **Trino/Presto OSS** | Free federated queries | Complex to operate, no managed option |

#### Our Unique Position

**No other platform combines all four capabilities:**

| Capability | Reiver | Query-Only Tools | Traditional APM | LLM Platforms |
|------------|-----------|------------------|-----------------|---------------|
| Federated data warehouse | Yes | Yes | No | No |
| Full APM/observability | Yes | No | Yes | No |
| LLM Gateway | Yes | No | No | Yes |
| AI Business Analyzer | Yes | No | No | No |

**What this means in practice:**

- **Query-only tools** (Spice.ai, GlareDB, MotherDuck): Can query your data, but can't monitor your application or route LLM requests
- **Traditional APM** (Datadog, New Relic): Can monitor your app, but can't query your business data from Stripe, Postgres, or CSV files
- **LLM platforms** (Portkey, Helicone): Can route LLM requests, but can't see your application traces or business data

**Reiver is the only platform where you can:**

1. See a slow API request in your traces
2. Query the underlying Postgres and Stripe data to understand why
3. Ask the AI analyzer: "Why are checkout failures increasing?"
4. Get an answer that correlates application errors with payment data and user behavior

This integrated approach is only possible because we've built unified access to heterogeneous data sources from the ground up.

### Platform Synergies: Insights No Single Tool Can Provide

The real competitive moat comes from combining APM, LLM Gateway, and Data Warehouse in ways no single-purpose tool can match.

#### 1. Performance Impact on Revenue

**The question**: "Did that slow API actually cost us money?"

| Data Source | What It Provides |
|-------------|------------------|
| APM | Which requests were slow, which users experienced them |
| Warehouse - Stripe | Customer ARR, payment history |
| Warehouse - CRM | Customer tier, account value |

**Unique insight**: "Last week's checkout latency spike (p99 > 3s) affected 847 users. Of those:
- 12 were enterprise customers ($50K+ ARR)
- 3 enterprise customers churned within 7 days
- Checkout conversion dropped 18% during the incident
- **Estimated revenue impact: $127,000**"

No other tool can answer: "What was the dollar cost of that outage?"

#### 2. True Unit Economics Per Request

| Data Source | What It Provides |
|-------------|------------------|
| APM metrics | Infrastructure cost per request (compute, memory, DB time) |
| LLM Gateway | AI cost per request (tokens, model pricing) |
| Warehouse | Revenue attribution (which customer, what they paid) |

**Unique insight**: "This API endpoint costs $0.12/request (infra + AI) but generates $4.50 in revenue. This other endpoint costs $0.08 but generates $0.02."

Calculate **profit margin per API call** - something no other tool can do.

#### 3. AI Model ROI Optimization

| Data Source | What It Provides |
|-------------|------------------|
| Gateway | Which model used, cost, latency, tokens |
| APM | User behavior after AI response (converted or abandoned?) |
| Warehouse | Actual business outcome (purchase, subscription, churn) |

**Unique insight**: "Claude-3-Opus costs 10x more than GPT-3.5, but users who get Opus responses have 23% higher conversion. ROI is positive for checkout flows but negative for FAQ responses."

**Action**: Automatically route expensive models only where they drive revenue.

#### 4. Predictive Infrastructure Scaling with Business Context

| Data Source | What It Provides |
|-------------|------------------|
| APM | Historical traffic patterns, resource utilization |
| Warehouse | Business calendar (campaigns, sales events) |
| Gateway | AI usage patterns (which features use expensive models) |

**Unique insight**: "Marketing just uploaded a campaign CSV targeting 50K users. Based on past campaigns, expect 3x traffic spike in 2 hours. Current AI budget will be exhausted in 4 hours."

#### 5. Full-Stack Customer Health Score

| Data Source | What It Provides |
|-------------|------------------|
| APM | Error rates, latency experienced, feature usage |
| Gateway | AI interaction quality (successful responses, retries) |
| Warehouse - Stripe | Payment history, failed charges, disputes |
| Warehouse - Zendesk | Support tickets, sentiment |
| Warehouse - Postgres | Account data, usage metrics |

**Unique insight**: "Enterprise customer Acme Corp health score: 34/100
- 12 unresolved support tickets
- 3x higher error rate than average
- 2 failed payment attempts
- Declining AI feature usage
- **Churn risk: HIGH. Alert customer success.**"

#### 6. Smart Alerting with Business Context

**Traditional APM alert**: "Error rate > 5%"

**Reiver alert**: "Error rate > 5% AND:
- Affecting customers with >$10K ARR
- During their business hours (timezone-aware from CRM)
- On checkout/payment flows
- When support queue is already at capacity"

Only alert on errors that actually impact revenue.

#### 7. AI Cost Attack Detection

| Data Source | What It Provides |
|-------------|------------------|
| Gateway | Token usage per user, model costs |
| APM | Request patterns, IPs, user agents |
| Warehouse | Customer billing tier, usage limits |

**Unique insight**: "User X made 500 AI requests in 10 minutes, costing $47. Their plan allows $10/month. Pattern matches prompt injection attack."

**Action**: Auto-rate-limit before costs explode.

#### 8. Developer Productivity → Business Impact

| Data Source | What It Provides |
|-------------|------------------|
| Warehouse - GitHub | Commits, PRs, deployments |
| APM | Error rates before/after deployment |
| Warehouse - Stripe | Revenue before/after changes |

**Unique insight**: "PR #1234 (refactored checkout flow):
- Reduced p99 latency by 200ms
- Error rate dropped 2.1% → 0.8%
- Checkout conversion increased 4.2%
- **Estimated revenue impact: +$12,400/month**"

#### 9. Cross-System Anomaly Detection

Detect when multiple metrics move together across systems:

| Anomaly Pattern | APM Signal | Gateway Signal | Warehouse Signal |
|-----------------|------------|----------------|------------------|
| Incident starting | Error spike | AI timeout increase | Support tickets surge |
| Fraud attack | Auth failures | Same prompts repeated | Chargebacks increasing |
| Feature broken | 500 errors | AI fallback triggering | Refund requests up |

Correlate anomalies across systems to detect incidents before customers report them.

#### 10. Revenue Leak Detection

| Data Source | What It Provides |
|-------------|------------------|
| APM | Successful API responses |
| Gateway | Successful AI generations |
| Warehouse - Stripe | Actual charges |

**Unique insight**: "1,247 users received premium AI responses but weren't charged. Revenue leak: $3,741/month. Root cause: webhook failure after Stripe API change on Jan 15."

#### 11. Complete Audit Trail for Compliance

| Data Source | What It Provides |
|-------------|------------------|
| APM | Who accessed what, when, from where |
| Gateway | What AI models processed what data |
| Warehouse | PII data access patterns |

**Unique insight**: Generate a complete GDPR Article 30 report: all systems that processed user X's data, including AI models, and what business decisions resulted.

#### 12. Security Vulnerability & Compliance Risk Detection

Beyond operational efficiency, Reiver's unified data access enables proactive detection of **security vulnerabilities** and **compliance risks** that would be invisible when data is siloed.

**Compliance Violation Detection:**

| Regulation | What We Detect | Data Sources Needed |
|------------|----------------|---------------------|
| **GDPR** | PII in logs, missing consent records, cross-border transfers to non-adequate countries | APM logs + CRM consent flags + Cloud provider metadata |
| **PCI DSS** | Credit card numbers in plain text, unauthorized cardholder data access | APM logs + Payment gateway + Access logs |
| **HIPAA** | PHI exposed in non-compliant systems, missing BAA coverage | Healthcare data + System inventory + Vendor agreements |
| **SOX** | Financial data access without audit trail, segregation of duties violations | Accounting systems + Access logs + HR org chart |
| **CCPA** | California consumer data retained beyond 12 months, incomplete deletion requests | CRM + Data warehouse + Deletion request logs |
| **Crypto Tax (IRS/Global)** | Unreported transactions, missing cost basis, wash sales, cross-chain transfers | Blockchain data + Invoices + Accounting systems |
| **Data Retention** | Data kept beyond policy limits across any system | All connected sources with timestamp metadata |

**Example - GDPR Violation Scan:**

```sql
-- Find PII that was processed by AI models without consent
SELECT u.email, g.model, g.prompt_preview, u.gdpr_consent_date
FROM gateway.llm_requests g
JOIN crm.users u ON g.user_id = u.id
WHERE g.prompt LIKE '%' || u.email || '%'
  AND (u.gdpr_consent_date IS NULL OR u.ai_processing_consent = false)
```

**Unique insight**: "47 users had their email addresses sent to GPT-4 without AI processing consent. 12 were EU residents. Remediation required."

**Example - Crypto Tax Compliance for Businesses:**

For businesses accepting cryptocurrency payments, tax reporting is a nightmare without unified data access. Reiver connects blockchain data with your invoicing and accounting systems:

```sql
-- Match on-chain payments to invoices for tax reporting
SELECT 
  i.invoice_id,
  i.customer_name,
  i.amount_usd AS invoice_amount,
  tx.value_usd AS received_amount,
  tx.tx_hash,
  tx.block_timestamp,
  tx.token_symbol,
  CASE 
    WHEN tx.value_usd >= i.amount_usd THEN 'Paid'
    WHEN tx.value_usd IS NULL THEN 'Unpaid'
    ELSE 'Partial'
  END AS payment_status
FROM accounting.invoices i
LEFT JOIN ethereum.transactions tx 
  ON tx.to_address = i.payment_wallet
  AND tx.block_timestamp BETWEEN i.issue_date AND i.due_date
WHERE i.payment_method = 'crypto'
  AND i.tax_year = 2025
```

**Unique insights for crypto businesses:**

| Use Case | What Reiver Provides |
|----------|------------------------|
| **IRS Form 8949** | Auto-generate capital gains/losses by joining wallet transactions with cost basis from accounting |
| **Revenue Recognition** | Match on-chain payments to invoices with USD conversion at time of receipt |
| **Multi-Chain Reconciliation** | Unify Ethereum, Solana, Bitcoin, Polygon transactions with a single query |
| **Cost Basis Tracking** | Calculate FIFO/LIFO cost basis across exchanges and wallets |
| **Wash Sale Detection** | Identify wash sales by correlating sell transactions with repurchases within 30 days |
| **Cross-Border Compliance** | Apply correct tax treatment based on customer jurisdiction from CRM |
| **Audit Trail** | Complete transaction history with fiat value at time of transaction |

**Example - End of Year Tax Summary:**

```
Reiver Tax Report: 2025 Crypto Revenue

Total crypto payments received: 847 transactions
- Ethereum: 412 transactions ($1.2M USD at time of receipt)
- Solana: 289 transactions ($340K USD)  
- Bitcoin: 146 transactions ($890K USD)

Matched to invoices: 834/847 (98.5%)
Unmatched transactions: 13 (review required)

Cost basis method: FIFO
Realized gains from payment conversions: $127,400
Unrealized gains (still held): $89,200

IRS Form 8949 ready for export ✓
```

**Security Vulnerability Detection:**

| Threat | Detection Method | Data Sources |
|--------|------------------|--------------|
| **Credential Exposure** | API keys, tokens, passwords in logs or prompts | APM logs + Gateway prompts |
| **SQL Injection Attempts** | Malicious patterns in query logs | Database slow query logs + APM |
| **Unusual Access Patterns** | Data access outside normal hours/locations | Access logs + HR schedule + IP geolocation |
| **Privilege Escalation** | Users accessing data outside their role | Access logs + HR roles + Data classification |
| **Data Exfiltration** | Large downloads by unusual accounts | API logs + Data warehouse access + User behavior baseline |
| **API Abuse** | Rate limit violations, scraping patterns | Gateway metrics + APM + User tier from CRM |
| **Anomalous LLM Usage** | Prompt injection, jailbreak attempts | Gateway prompts + Known attack patterns |

**Example - Credential Leak Detection:**

```
Reiver Alert: Potential credential exposure detected

1. Scanned 2.4M log entries across APM and Gateway
2. Found 3 instances of API keys matching pattern 'sk-...'
3. Cross-referenced with secrets inventory
4. Result: Production Stripe key exposed in error log
   - First occurrence: 2024-01-15 14:23:01
   - Affected systems: checkout-service, payment-worker
   - Recommendation: Rotate key immediately, audit access logs
```

**Example - Insider Threat Detection:**

| Data Source | What It Provides |
|-------------|------------------|
| APM | API access patterns, query logs |
| Warehouse - HR | Employee role, department, termination date |
| Warehouse - CRM | Customer data classification |
| Gateway | AI queries with customer context |

**Unique insight**: "Employee X (terminated 3 days ago) accessed 847 customer records in their last hour. Pattern matches data exfiltration. Account was not deprovisioned - escalate to security team."

**Why Only Reiver Can Do This:**

| Capability | Single-Purpose Tools | Reiver |
|------------|----------------------|-----------|
| Detect PII in logs | Partial (no business context) | Cross-reference with consent records |
| Identify unauthorized access | Flag anomalies | Match against HR roles + data classification |
| Compliance audit | Manual, weeks of work | Real-time, automated scans |
| Credential exposure | Regex pattern matching | + Secrets inventory + Rotation status |
| Cross-system violations | Impossible (data silos) | Unified query across all systems |
| Crypto tax reporting | Export CSVs, manual matching | Auto-match on-chain payments to invoices |
| Multi-chain reconciliation | Separate tools per chain | Single SQL query across all blockchains |

This transforms compliance from a quarterly audit nightmare into continuous, automated monitoring.

#### Summary: Why Integration Matters

| Insight | Required Data | Single-Purpose Tools | Reiver |
|---------|---------------|----------------------|-----------|
| Cost of a slow API | APM + CRM + Billing | Manual correlation | Instant answer |
| Profit per request | Infra costs + AI costs + Revenue | Impossible | Calculated automatically |
| AI model ROI | Gateway + Conversions + Revenue | Can't connect them | Optimizes automatically |
| Customer churn prediction | Errors + Tickets + Payments + Usage | 4 separate dashboards | Single health score |
| Revenue impact of PR | GitHub + APM + Stripe | Engineering guess | Precise dollar amount |
| Compliance audit | Logs + AI access + Data warehouse | Weeks of work | One query |

### Hybrid Storage Strategy



We use a **hybrid approach** optimized for different data ownership scenarios:

| Data Type | Storage | Query Method | Why |
|-----------|---------|--------------|-----|
| **Synced Data** (Stripe, Postgres, etc.) | Native ClickHouse | MergeTree tables | Best query performance, native indexes |
| **Client-Stored Data** (customer's S3/R2) | Customer's object storage | FST index + s3() | No data movement, customer retains control |

```
Synced Data Flow:
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ External Source │ ──▶ │   Sync Worker   │ ──▶ │   ClickHouse    │
│ (Stripe, etc.)  │     │                 │     │  (Native Tables)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
                                                   Fast Queries
                                                (Native Indexes)

Client-Stored Data Flow:
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Customer's S3   │ ◀── │   FST Index     │ ◀── │  Query Rewriter │
│ (Parquet files) │     │ (File Filtering)│     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │
         ▼                       │
┌─────────────────┐              │
│   ClickHouse    │ ◀────────────┘
│    s3() query   │  (Only reads filtered files)
└─────────────────┘
```

### FST Skip Index (Client-Stored Data)

For data stored in customer's object storage, ClickHouse cannot use native indexes since data lives outside its control. We built an **FST (Finite State Transducer) based skip index** to optimize queries:

**The Problem**: Without optimization, a query like `SELECT * FROM orders WHERE status = 'active'` would scan ALL Parquet files in the customer's S3 bucket (potentially 100,000+ files).

**Our Solution**: The FST index tracks which values exist in which files. Before querying, we filter to only the files that might contain matching rows.

| Query Type | Without FST Index | With FST Index |
|------------|-------------------|----------------|
| `WHERE date = '2025-01-15'` | Partition pruning only | Same |
| `WHERE status = 'active'` | Scan all files | Skip files without 'active' |
| `WHERE customer_id = 'abc'` | Scan all files | Skip files without 'abc' |
| `WHERE name LIKE 'John%'` | Scan all files | FST prefix query |

**Why FST over alternatives?**

| Feature | FST | HashSet | Bloom Filter |
|---------|-----|---------|--------------|
| Exact membership | Yes | Yes | No (false positives) |
| Prefix queries | Yes | No | No |
| Range queries | Yes | No | No |
| Memory efficiency | Excellent | Poor | Good |

### Storage Cost Comparison

| Storage Option | Cost per TB/month | Notes |
|----------------|-------------------|-------|
| ClickHouse Cloud (managed) | $100-200 | Includes compute, replication |
| ClickHouse (self-hosted, Hetzner/OVH) | $45-60 | SSD block storage |
| ClickHouse (self-hosted, AWS EBS gp3) | ~$80 | |
| Cloudflare R2 | $15 | Zero egress fees |
| AWS S3 | $23 | + egress costs |

**Our cost structure**:
- Synced data in native ClickHouse: ~$45-60/TB (self-hosted) for best performance
- Client-stored data in R2/S3: $15-23/TB with FST optimization for good performance

### Why ClickHouse for External Data?

Even though we build our own FST index for file selection, ClickHouse provides significant value for query execution:

| Capability | What ClickHouse Provides |
|------------|-------------------------|
| Parallel S3 downloads | 100+ concurrent connections |
| Vectorized execution | SIMD-optimized columnar processing |
| Row group statistics | Skip row groups within Parquet files |
| Column pruning | Only read requested columns |
| Complex SQL | JOINs, aggregations, window functions |
| Memory management | Spill to disk for large results |

**Division of responsibility**:
- **FST Index**: Answers "which files to read?" (100K → 50 files)
- **ClickHouse**: Answers "how to read them fast?" (parallel, vectorized, columnar)

### Performance Expectations

| Scenario | Synced Data (Native) | Client-Stored (FST + s3()) |
|----------|---------------------|---------------------------|
| Simple SELECT | 10-50ms | 100-500ms |
| Aggregation (1M rows) | 50-200ms | 200-1000ms |
| JOIN (2 tables) | 100-500ms | 500-2000ms |
| Full table scan | Rare (indexes help) | Avoided by FST |

### FST Index Performance Benchmarks

We've benchmarked the FST skip index to validate its performance at scale. The key finding: **FST with partition hints provides constant-time query filtering regardless of data volume**.

#### Scaling Behavior (large_scale_fst benchmark)

| Files | FST + Partition | FST Only | Speedup |
|-------|-----------------|----------|---------|
| 5,000 | 38.4 µs | 158 µs | 4.1x |
| 10,000 | 38.2 µs | 164 µs | 4.3x |
| 25,000 | 38.4 µs | 162 µs | 4.2x |
| 50,000 | 38.0 µs | 157 µs | 4.1x |
| **100,000** | **38.2 µs** | **158 µs** | **4.1x** |

**Key insight**: Time stays constant at ~38 µs regardless of data size when using partition hints. This is O(1) lookup performance.

#### Partition Hint Impact (skip_index_filter benchmark)

| Files | No Hint | With Hint | Speedup |
|-------|---------|-----------|---------|
| 10,000 | 179 µs | 28 µs | **6.4x** |
| 100,000 | 185 µs | 28 µs | **6.6x** |
| 500,000 | 1.25 ms | 153 µs | **8.2x** |

Partition hints (date-based pruning) provide 6-8x speedup by limiting the search to relevant partitions.

#### FST Build Cost

| Cardinality | Build Time | Throughput |
|-------------|------------|------------|
| 100 values | 31 µs | 3.2M elem/s |
| 1,000 values | 92 µs | 10.9M elem/s |
| 10,000 values | 706 µs | 14.2M elem/s |
| 50,000 values | 3.4 ms | 14.8M elem/s |

Building FST indexes is fast - even 50K unique values only takes 3.4ms.

#### Running Benchmarks

```bash
# Run all warehouse benchmarks
cargo bench --bench warehouse_benchmarks

# Run specific benchmark groups
cargo bench --bench warehouse_benchmarks -- skip_index_filter   # Core FST filtering
cargo bench --bench warehouse_benchmarks -- large_scale_fst     # Scale comparison
cargo bench --bench warehouse_benchmarks -- data_types          # Numeric, timestamp, boolean
cargo bench --bench warehouse_benchmarks -- cardinality         # Low to high cardinality
cargo bench --bench warehouse_benchmarks -- table_shape         # Narrow to ultra-wide tables
cargo bench --bench warehouse_benchmarks -- realworld           # E-commerce, events, IoT
cargo bench --bench warehouse_benchmarks -- edge_cases          # Skewed, unicode, long strings
cargo bench --bench warehouse_benchmarks -- memory              # Memory pressure tests
```

#### Benchmark Categories

| Category | What It Tests | Key Metrics |
|----------|---------------|-------------|
| **data_types** | Numeric, timestamp, boolean, mixed columns | FST behavior with non-string data |
| **cardinality** | 3 values → 50K+ values | Where FST becomes ineffective |
| **table_shape** | 5 → 500 columns per table | Column selection overhead |
| **realworld** | E-commerce, event logs, time-series | Production query patterns |
| **edge_cases** | Skewed distributions, unicode, long strings | Boundary conditions |
| **memory** | Near 100K cardinality limit, partition scaling | Memory pressure behavior |

#### Data Patterns Tested

We validate FST performance across diverse real-world scenarios:

**E-commerce Pattern:**
- Order status, payment method, shipping (low cardinality)
- Country, product category (medium cardinality)
- Order ID, customer ID (high cardinality - skip FST)

**Event/Log Pattern:**
- Event type, platform, browser (low cardinality)
- Page path, country (medium cardinality)
- Session ID, event ID (high cardinality - skip FST)

**Time-series Pattern:**
- Metric name, location, status (low cardinality)
- Device ID (medium cardinality)
- Timestamp partitioning

#### Why This Matters

Compared to competitors like PostHog who scan all Parquet files:

- **Reiver with FST**: ~38 µs to filter 100K files → read only matching files
- **Full scan approach**: Must open and check every file → O(n) cost

This enables sub-second queries on datasets with millions of files, making Reiver's "bring your own data" tier competitive with native storage performance.

### Realistic I/O Benchmark Results

We benchmark FST performance with real Parquet files stored in S3-compatible storage (MinIO) to measure actual I/O impact:

```bash
make warebench   # Run the realistic benchmark
```

#### Results Summary (500 files, 5M rows)

| Query Type | Selectivity | FST Time | No-Index Time | Speedup |
|------------|-------------|----------|---------------|---------|
| High-cardinality lookup | 0.2% | 1.2ms | 521ms | **438x** |
| Prefix match (`LIKE 'cust_0000%'`) | 0.2% | 1.2ms | 536ms | **440x** |
| Multi-column AND | 3.4% | 10ms | 539ms | **53x** |
| Rare value combination | 1.8% | 5.7ms | 554ms | **97x** |
| User segmentation | 6.8% | 20ms | 572ms | **28x** |
| Medium cardinality | 6.6% | 20ms | 545ms | **27x** |
| Numeric range | 50% | 133ms | 528ms | **4x** |
| Boolean filter | 20% | 55ms | 535ms | **10x** |
| Low cardinality | 33% | 123ms | 592ms | **4.8x** |
| No match (0%) | 0% | 1.2µs | 535ms | **442,644x** |
| All match (100%) | 100% | 270ms | 546ms | **2x** |

**Aggregate Statistics:**
- Overall speedup: **6.6x** across all query types
- String column queries: **157x** average speedup
- Multi-column queries: **28x** average speedup
- Numeric queries: **7x** average speedup

### FST Performance Formula

The speedup from FST indexing can be predicted using this formula:

```
Speedup = T_no_index / T_fst

Where:
  T_no_index = N × T_scan + M × T_read    (scan all files, read matches)
  T_fst      = T_lookup + M × T_read       (O(1) lookup, read matches)

  N = total number of files
  M = matching files = N × selectivity
  T_scan = time to fetch and parse one file's metadata (~50-100ms over network)
  T_read = time to read one file's data (~50-100ms over network)
  T_lookup = FST lookup time (~1-50µs, negligible)
```

**Simplified Formula:**

Since `T_lookup ≈ 0` and `T_scan ≈ T_read`:

```
Speedup ≈ (N × T + M × T) / (M × T)
        = (N + M) / M
        = N/M + 1
        = 1/selectivity + 1

For low selectivity (S << 1):
  Speedup ≈ 1/S
```

#### Speedup by Selectivity

| Selectivity | Expected Speedup | Actual (Benchmark) |
|-------------|------------------|-------------------|
| 0.2% | ~500x | 438-440x |
| 1% | ~100x | 97x |
| 3% | ~33x | 53x |
| 7% | ~14x | 27-28x |
| 20% | ~5x | 10x |
| 33% | ~3x | 4.8x |
| 50% | ~2x | 4x |
| 100% | ~1x | 2x |

**Note:** Actual speedups often exceed theoretical predictions because:
1. FST lookup is O(1) while no-index must scan sequentially (even with parallelism)
2. Network overhead is amortized better with fewer requests
3. Memory caching effects favor smaller result sets

#### Cardinality Impact on Selectivity

Cardinality directly affects selectivity for equality queries:

```
selectivity_equality = 1 / cardinality

Examples:
  status (3 values):      selectivity = 33%  → 3x speedup
  region (20 values):     selectivity = 5%   → 20x speedup
  customer_id (100K):     selectivity = 0.001% → 100,000x speedup
```

#### Multi-Column Query Effects

**AND queries** multiply selectivity (intersection):
```
selectivity_AND = S1 × S2 × ... × Sn

Example: status='active' AND region='us-east'
  S1 = 33%, S2 = 5%
  Combined = 1.65% → ~60x speedup
```

**OR queries** add selectivity (union):
```
selectivity_OR ≈ S1 + S2 - (S1 × S2)

Example: status='active' OR status='pending'
  S1 = 33%, S2 = 33%
  Combined ≈ 56% → ~2x speedup
```

#### When FST Provides Maximum Value

| Scenario | Selectivity | Speedup | Example |
|----------|-------------|---------|---------|
| **High-cardinality exact match** | < 0.1% | 100-1000x | `user_id = 'abc123'` |
| **Prefix search** | < 1% | 100-500x | `email LIKE 'john%'` |
| **Multi-column AND** | 1-5% | 20-100x | `status = 'x' AND region = 'y'` |
| **Medium cardinality** | 5-10% | 10-20x | `country = 'US'` |
| **Low cardinality** | 20-50% | 2-5x | `status = 'active'` |
| **No match** | 0% | ∞ | Invalid filter value |

#### When FST Provides Minimal Value

| Scenario | Selectivity | Speedup | Recommendation |
|----------|-------------|---------|----------------|
| **All match** | 100% | 1-2x | Skip FST check |
| **Very low cardinality** | > 50% | < 2x | Consider other optimizations |
| **High-cardinality columns** | N/A | N/A | Don't build FST (memory cost) |

**FST is automatically skipped for:**
- Columns with > 100,000 unique values (UUIDs, timestamps)
- Columns where FST size would exceed 50MB

### Memory Safety

The FST index includes safeguards to prevent OOM:

```rust
const MAX_SUMMARY_CARDINALITY: usize = 100_000;  // Skip high-cardinality columns
const MAX_SUMMARY_MEMORY_BYTES: usize = 50 * 1024 * 1024;  // 50MB limit per FST
```

High-cardinality columns (UUIDs, timestamps) are automatically excluded from indexing.

### Pricing Model

The hybrid architecture enables differentiated pricing based on where data is stored:

#### Our Cost Structure

| Cost Component | Synced Data | Client-Stored Data |
|----------------|-------------|-------------------|
| Storage | $45-60/TB (ClickHouse) | $0 (customer pays) |
| FST index storage | N/A | $1-2/TB (PostgreSQL) |
| Sync compute | $5-10/TB (workers) | N/A |
| Query compute | Included in cluster | $5-10/TB (amortized) |
| **Total our cost** | **~$50-70/TB/month** | **~$10-15/TB/month** |

#### Pricing Tiers

| Tier | What's Included | Best For |
|------|-----------------|----------|
| **Synced Data** | We store in optimized ClickHouse, sub-50ms latency, full indexing | Dashboards, real-time analytics |
| **Bring Your Own Data** | Query customer's S3/R2 directly, 100-500ms latency, FST optimization | Large datasets, compliance requirements |

**Price ratio**: Client-stored should be ~3x cheaper than synced data, reflecting:
- No storage cost for us (customer owns the data)
- Slightly higher query latency
- Customer retains data ownership and compliance responsibility

#### Minimum Viable Pricing (Client-Stored)

```
FST index storage:        $1-2/TB
ClickHouse query compute: $5-10/TB (amortized)
Infrastructure margin:    $5-10/TB
──────────────────────────────────
Minimum break-even:       ~$15-20/TB/month
```

Below $20/TB for client-stored data, we operate at a loss.

## Enterprise Features

- **SSO/SAML**: Okta, Auth0, Entra ID, OneLogin, Ping, Keycloak
- **SCIM**: Automatic user provisioning from identity providers
- **MFA**: TOTP and WebAuthn/passkey support
- **Notifications**: Slack, PagerDuty, Discord, Teams, ServiceNow, webhooks
- **Self-Hostable**: Run on your infrastructure with full data control

## Documentation

See the [docs/](docs/) folder for detailed documentation:

- [API Reference](docs/api.md)
- [SDK Documentation](docs/sdk.md)
- [Deployment Guide](docs/deployment.md)
- [Configuration Reference](docs/configuration.md)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

## Federated Query Architecture: Solved Challenges

Querying heterogeneous data sources (CSV files, Stripe API, Parquet in S3, PostgreSQL, etc.) and joining them in a single query presents significant engineering challenges. This section documents the problems we've solved and the architecture behind our solutions.

### Overview: The Federation Problem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           User Query                                         │
│  SELECT s.*, p.*, c.*                                                        │
│  FROM stripe.charges s                                                       │
│  JOIN parquet_events.clicks p ON s.customer_id = p.user_id                  │
│  JOIN csv_data.marketing c ON p.campaign_id = c.id                          │
│  WHERE p.event_date > '2024-01-01'                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SOLUTIONS IMPLEMENTED                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Type Coercion      ✅ Arrow types + semantic metadata + coercion rules  │
│  2. Schema Matching    ✅ Cross-source reconciliation with warnings         │
│  3. NULL Semantics     ✅ Configurable per-source NULL handling             │
│  4. Date/Time Handling ✅ Multi-format parsing, timezone normalization      │
│  5. Join Strategy      ✅ Cost-based planning with semi-join optimization   │
│  6. Predicate Pushdown ✅ Source capability matrix for 40+ connectors       │
│  7. Unified Catalog    ✅ Schema discovery, lineage, statistics             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Execution Engine                                     │
│                    (ClickHouse + Materialization)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Solution 1: Type System with Arrow + Semantic Types

Each data source has its own type system with different semantics:

| Source | Type System | Key Differences |
|--------|-------------|-----------------|
| **CSV** | Strings (inferred) | Everything is text; numbers/dates are guessed |
| **Stripe API** | JSON-based | Amounts in cents (Int64), timestamps as Unix seconds |
| **Parquet** | Arrow types | Rich precision (Decimal128), nested structures |
| **PostgreSQL** | SQL types | Arrays, enums, intervals, custom domains |
| **ClickHouse** | Native types | Nullable wrappers, LowCardinality optimization |

**Problem Example**: Joining `stripe.charges.amount` (Int64 cents) with `postgres.orders.total` (DECIMAL(10,2) dollars):

```sql
-- What should happen here?
SELECT * FROM stripe.charges s
JOIN postgres.orders o ON s.amount = o.total  -- 1999 cents vs 19.99 dollars!
```

**Our solution**:
- Semantic types detect cents vs dollars and require explicit conversion functions
- Users get clear error messages with suggested fixes (e.g., `cents_to_dollars()`)
- Automatic type coercion with warnings for potential precision loss

**Implementation**: Our type system in `src/warehouse/types.rs` uses Arrow types with full precision:

```rust
pub enum ArrowDataType {
    Decimal128(precision, scale),  // Preserves DECIMAL(18,4)
    Timestamp(precision, timezone), // Preserves microseconds + timezone
    // ... full Arrow type system
}

pub enum SemanticType {
    Money { currency: String, in_cents: bool },
    Percentage { range: PercentageRange },
    Identifier,
    // ... semantic annotations
}
```

PostgreSQL `DECIMAL(18,4)` is preserved as `Decimal128(18, 4)` with optional `SemanticType::Money`.

### Solution 2: Schema Reconciliation for JOINs

When joining across sources, schemas must be compatible:

```sql
SELECT * FROM stripe.customers c
JOIN csv_data.users u ON c.email = u.email_address
JOIN postgres.accounts a ON u.id = a.user_id
```

**Problems to solve**:

| Issue | Example | Impact |
|-------|---------|--------|
| **Column naming** | `email` vs `email_address` | User must handle (aliases) |
| **Case sensitivity** | PostgreSQL lowercases, others don't | Silent mismatches |
| **Type compatibility** | String email vs VARCHAR(255) | Usually works |
| **Key type mismatch** | UUID string vs UUID native | May fail silently |
| **Collation differences** | Case-sensitive vs insensitive | Wrong JOIN results |

**Our solution** (implemented in `query/schema_reconciliation.rs`):
- Case sensitivity differences detected and warned
- Automatic type coercion with `JoinKeyCompatibility` analysis
- UUID format mismatches (string vs binary) handled with warnings
- Semantic type conflicts (cents vs dollars) blocked with actionable errors

### Solution 3: Configurable NULL Semantics

Different sources handle NULL/missing values differently:

| Source | NULL Representation | Semantics |
|--------|---------------------|-----------|
| **PostgreSQL** | `NULL` | Tri-valued logic (NULL != NULL) |
| **CSV** | Empty string `""` | Is this NULL or empty string? |
| **CSV** | Literal `"NULL"` | Is this NULL or the word "NULL"? |
| **CSV** | Missing column | Field not in row |
| **JSON/API** | `null` | Explicit null value |
| **JSON/API** | Missing field | Field not present in response |
| **Stripe** | Field absent | Not applicable vs unknown |

**Problem Example**:

```sql
-- CSV has empty email field, PostgreSQL has NULL email
SELECT * FROM csv.users c
JOIN postgres.customers p ON c.email = p.email
WHERE c.email IS NOT NULL
```

- Does empty string `""` in CSV pass the `IS NOT NULL` check?
- Should empty CSV fields be treated as NULL?
- How do we configure this per-source?

**Our solution** (implemented in `types.rs` and `sources/types.rs`):

```rust
pub struct NullSemantics {
    pub treat_empty_as_null: bool,      // Default: false (empty string is valid)
    pub null_literals: Vec<String>,      // ["NULL", "NA", "N/A"] treated as NULL
    pub whitespace_is_null: bool,        // "   " treated as NULL
}
```

- Default: Empty strings are valid values, not NULL (prevents data loss)
- Configurable per-source via `SourceConfig::File { null_semantics: ... }`
- Legacy mode available for backwards compatibility

### Solution 4: Date/Time Normalization

Date and time handling varies dramatically across sources:

| Source | Format | Timezone | Precision |
|--------|--------|----------|-----------|
| **CSV** | String `"2024-01-15"` or `"01/15/2024"` | None (assume UTC?) | Day |
| **CSV** | String `"2024-01-15T10:30:00Z"` | Explicit in string | Seconds |
| **Stripe** | Unix timestamp `1705312800` | Always UTC | Seconds |
| **PostgreSQL** | `timestamp` | Session timezone | Microseconds |
| **PostgreSQL** | `timestamptz` | Stored as UTC | Microseconds |
| **Parquet** | `Timestamp(us, None)` | None | Microseconds |
| **Parquet** | `Timestamp(ns, Some("UTC"))` | UTC | Nanoseconds |

**Problem Examples**:

```sql
-- Stripe timestamp (seconds) vs Parquet (microseconds)
SELECT * FROM stripe.charges s
JOIN parquet.events p ON s.created = p.event_time  -- 1705312800 vs 1705312800000000

-- CSV date string vs PostgreSQL timestamptz
SELECT * FROM csv.campaigns c
JOIN postgres.orders o ON c.start_date = o.created_at  -- "2024-01-15" vs 2024-01-15 10:30:00+00
```

**Our solution** (implemented in `connectors/date_parsing.rs` and `types.rs`):

```rust
// Multi-format date detection
pub fn detect_date_format(samples: &[&str]) -> DateFormat {
    // Tries ISO 8601, US format, European format, Unix timestamps
}

// Precision-preserving timestamp coercion
pub enum TimestampPrecision { Second, Millisecond, Microsecond, Nanosecond }
// Arrow handles automatic precision alignment during JOINs
```

- Standardized on Arrow `Timestamp(Microsecond, tz)` internally
- Automatic precision alignment (seconds → microseconds) with no data loss
- Multi-format CSV parsing with pattern detection
- `DATE = TIMESTAMP` comparisons expand to day range with warning

### Solution 5: Cost-Based Join Planning

Data sources have vastly different access characteristics:

| Source | Location | Latency | Bandwidth | Parallelism |
|--------|----------|---------|-----------|-------------|
| **ClickHouse native** | Local SSD | 1-10ms | 10+ GB/s | High |
| **Parquet in S3** | Network | 50-200ms | 100+ MB/s | High |
| **PostgreSQL** | Network | 10-50ms | 50-200 MB/s | Medium |
| **Stripe API** | Internet | 100-500ms | 1-10 MB/s | Low (rate limits) |
| **CSV file** | Local/S3 | 1-200ms | Varies | Low |

**Problem Example**:

```sql
SELECT s.*, p.* 
FROM stripe.charges s              -- 10M rows, API-limited
JOIN parquet_events.clicks p       -- 1B rows in S3
ON s.customer_id = p.user_id
WHERE p.event_date > '2024-01-01'  -- Reduces to 10M rows
```

**Decisions needed**:
1. Push `event_date` predicate to Parquet first? (Reduces 1B → 10M)
2. Materialize Stripe data into temp table? (Avoid repeated API calls)
3. Which side is the "build" side for hash join?
4. How to estimate cardinality for unknown sources?

**Our solution** (implemented in `src/warehouse/query/federation.rs`):

```rust
pub enum CombinationStrategy {
    None,                    // Single source
    DirectMerge { ... },     // ClickHouse handles all
    PushdownJoin { ... },    // Materialize smaller side
    MaterializeJoin { ... }, // Materialize all sources
    Union { ... },           // Simple UNION
}
```

**Implementation details**:
- Cost model in `cost_model.rs` weighs network I/O vs compute
- Statistics collected during sync and stored in `statistics/` module
- Semi-join optimization reduces data transfer (10K key IN clause or Bloom filter)
- Adaptive execution with circuit breaker for runaway queries
- Memory budget for materialization

### Solution 6: Source-Aware Predicate Pushdown

Different sources support different query capabilities:

| Source | Supported Operations | Limitations |
|--------|---------------------|-------------|
| **PostgreSQL** | Full SQL (WHERE, GROUP BY, HAVING, functions) | None |
| **Stripe API** | Limited filters (`created[gte]`, `customer`) | No arbitrary predicates |
| **CSV file** | None | Must scan all rows |
| **Parquet** | Column pruning, row group stats | No complex predicates |
| **ClickHouse** | Full SQL | None |

**Problem Example**:

```sql
SELECT * FROM stripe.charges
WHERE amount > 1000 
  AND created > '2024-01-01'
  AND metadata->>'source' = 'web'
```

- `created > '2024-01-01'` → Stripe API supports this
- `amount > 1000` → Must filter after fetching
- `metadata->>'source'` → Must filter after fetching

**Our solution** (implemented in `query/source_capabilities.rs` and `query/predicate_pushdown.rs`):

```rust
// 40+ sources with detailed capability matrices
pub struct SourceCapabilities {
    supported_operations: HashSet<FilterOperation>,
    column_filters: HashMap<String, ColumnFilterCapability>,  // Per-column rules
    supports_and: bool, supports_or: bool,
    full_scan_cost_multiplier: f64,  // For cost-based decisions
}
```

- `PredicateSplitter` routes each predicate to sources that support it
- Warnings generated for non-pushable predicates on large sources
- Value transforms applied (e.g., `TimestampToEpoch` for Stripe API)

### Solution 7: Unified Catalog with Lineage

To write cross-source queries, users need:

| Requirement | Description | Implementation |
|-------------|-------------|----------------|
| **Schema discovery** | List all tables and columns across sources | ✅ `catalog/discovery/` with per-source discoverers |
| **Type documentation** | What type is `stripe.charges.amount`? | ✅ `TypedSchema` with Arrow types + semantic metadata |
| **Statistics** | Row counts, cardinality, min/max | ✅ `statistics/` module with persistence |
| **Freshness** | When was this source last synced? | ✅ `FreshnessInfo` with sync timestamps |
| **Lineage** | Where did this column come from? | ✅ `ColumnLineage` tracking source → target |
| **Relationships** | Foreign keys across sources | ✅ `CrossSourceRelationship` with validation |

**Implementation** in `src/warehouse/catalog/`:

```rust
// catalog/service.rs - High-level API
pub struct CatalogService {
    repository: CatalogRepository,      // PostgreSQL persistence
    statistics_repo: StatisticsRepository,
    discovery: HashMap<SourceType, Box<dyn SchemaDiscovery>>,
}

// catalog/types.rs - Rich metadata
pub struct CatalogEntry {
    schema: TypedSchema,           // Full Arrow schema with semantic types
    statistics: TableStatistics,   // Cardinality, histograms
    freshness: FreshnessInfo,      // Last sync, staleness
    lineage: Vec<ColumnLineage>,   // Data provenance
    relationships: Vec<CrossSourceRelationship>,  // Inferred foreign keys
}
```

**Implementation details**:
- Schema stored in PostgreSQL with JSONB for flexibility
- Statistics refreshed during sync and on-demand via `StatisticsCollector`
- Cross-source relationships inferred by `RelationshipInference` (name + type matching)
- Query hints supported via `CatalogEntry.statistics`

### Summary: Implementation Status

| Problem | Status | Implementation |
|---------|--------|----------------|
| **Type coercion** | ✅ Solved | `types.rs`: Arrow-based type system with `coerce_types()`, semantic types |
| **JOIN key compatibility** | ✅ Solved | `query/schema_reconciliation.rs`: 1500+ lines handling all edge cases |
| **NULL semantics** | ✅ Solved | `NullSemantics` enum with configurable empty string handling |
| **Date/time handling** | ✅ Solved | `connectors/date_parsing.rs`: Multi-format parsing, timezone handling |
| **Join performance** | ✅ Solved | `query/federation.rs`: Cost-based planning with semi-join optimization |
| **Predicate pushdown** | ✅ Solved | `query/predicate_pushdown.rs`: 3000+ lines, source-aware splitting |
| **Unified catalog** | ✅ Solved | `catalog/service.rs`: Schema discovery, lineage, cross-source relationships |
| **Source capabilities** | ✅ Solved | `query/source_capabilities.rs`: 40+ source capability matrices |
| **Statistics collection** | ✅ Solved | `statistics/` module: Cardinality, min/max, histogram collection |
| **Cost estimation** | ✅ Solved | `query/cost_model.rs`, `cost_estimator.rs`: Network-aware planning |

### Architecture Highlights

The following components work together to enable seamless cross-source queries:

1. **Type system with precision** (`types.rs`) - Arrow-based types preserve source precision (e.g., `Decimal128(18,4)`)
2. **Safe type coercion** (`coerce_types()`) - Explicit, documented coercion with warnings for precision loss
3. **Source capability matrix** (`source_capabilities.rs`) - Defines what filters each of 40+ sources support
4. **Cost-based join planning** (`federation.rs`) - Chooses optimal strategy based on cardinality and network costs
5. **Predicate splitter** (`predicate_pushdown.rs`) - Routes filters to sources that can handle them natively
6. **Unified schema catalog** (`catalog/`) - Single view with lineage, relationships, and freshness tracking
7. **Statistics infrastructure** (`statistics/`) - Collects and persists cardinality, histograms for planning

---

## Type System Reference

Reiver uses [Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html) as the canonical internal type system. All source types are mapped to Arrow types, with optional semantic metadata for domain-specific meaning.

### Why Arrow?

- **Rich type system**: Supports precision/scale for decimals, timezone-aware timestamps, nested types
- **Industry standard**: Used by Parquet, DataFusion, DuckDB, ClickHouse, Pandas, Polars
- **Zero-copy operations**: Efficient data transfer between systems
- **Battle-tested**: Mature, well-documented, and widely adopted

### PostgreSQL Type Mapping

PostgreSQL has one of the richest type systems among databases. Here's how we map each type to Arrow:

#### Numeric Types

| PostgreSQL Type | Arrow DataType | Precision | Notes |
|-----------------|----------------|-----------|-------|
| `smallint` / `int2` | `Int16` | Exact | -32,768 to 32,767 |
| `integer` / `int4` | `Int32` | Exact | -2B to 2B |
| `bigint` / `int8` | `Int64` | Exact | Full 64-bit range |
| `smallserial` | `Int16` | Exact | Auto-increment, treated as Int16 |
| `serial` | `Int32` | Exact | Auto-increment, treated as Int32 |
| `bigserial` | `Int64` | Exact | Auto-increment, treated as Int64 |
| `real` / `float4` | `Float32` | ~7 digits | IEEE 754 single precision |
| `double precision` / `float8` | `Float64` | ~15 digits | IEEE 754 double precision |
| `numeric(p,s)` / `decimal(p,s)` | `Decimal128(p, s)` | Exact | Up to 38 digits precision |
| `numeric` (no precision) | `Decimal128(38, 18)` | Varies | Arbitrary precision, map to max |
| `money` | `Decimal128(19, 2)` + `SemanticType::Money` | Exact | Locale-dependent, 2 decimal places |

#### Character Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `char(n)` / `character(n)` | `Utf8` | Fixed-length, space-padded |
| `varchar(n)` / `character varying(n)` | `Utf8` | Variable-length with limit |
| `text` | `Utf8` | Unlimited length |
| `name` | `Utf8` | 64-byte internal identifier |

#### Binary Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `bytea` | `Binary` | Variable-length binary |
| `bit(n)` | `Binary` | Fixed-length bit string |
| `bit varying(n)` | `Binary` | Variable-length bit string |

#### Date/Time Types

| PostgreSQL Type | Arrow DataType | Precision | Notes |
|-----------------|----------------|-----------|-------|
| `date` | `Date32` | Day | Calendar date (no time) |
| `time` | `Time64(Microsecond)` | μs | Time of day without timezone |
| `time with time zone` | `Time64(Microsecond)` | μs | Time with timezone offset |
| `timestamp` | `Timestamp(Microsecond, None)` | μs | Date and time, no timezone |
| `timestamp with time zone` | `Timestamp(Microsecond, Some(tz))` | μs | Date and time with timezone |
| `interval` | `Duration(Microsecond)` + `SemanticType::Duration` | μs | Time span |

**Important**: PostgreSQL stores microsecond precision for timestamps. When joining with sources that use different precisions (milliseconds, seconds), Arrow handles auto-conversion to the higher precision.

#### Boolean Type

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `boolean` | `Boolean` | true/false/null |

#### UUID Type

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `uuid` | `FixedSizeBinary(16)` | 128-bit UUID |

#### JSON Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `json` | `Utf8` | Stored as text (validated JSON) |
| `jsonb` | `Utf8` | Binary JSON, stored as text for interop |

**Note**: We store JSON as `Utf8` for maximum compatibility. JSON field extraction happens at query time.

#### Array Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `integer[]` | `List(Int32)` | Array of integers |
| `text[]` | `List(Utf8)` | Array of strings |
| `<type>[]` | `List(<arrow_type>)` | Array of any type |

#### Network Address Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `inet` | `Utf8` | IPv4 or IPv6 host address |
| `cidr` | `Utf8` | IPv4 or IPv6 network address |
| `macaddr` | `Utf8` | MAC address |
| `macaddr8` | `Utf8` | MAC address (EUI-64 format) |

#### Geometric Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `point` | `Struct({x: Float64, y: Float64})` | 2D point |
| `line` | `Utf8` | Infinite line (stored as text) |
| `lseg` | `Utf8` | Line segment |
| `box` | `Utf8` | Rectangular box |
| `path` | `Utf8` | Path (open or closed) |
| `polygon` | `Utf8` | Polygon |
| `circle` | `Utf8` | Circle |

#### Range Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `int4range` | `Struct({lower: Int32, upper: Int32, ...})` | Range of integers |
| `int8range` | `Struct({lower: Int64, upper: Int64, ...})` | Range of bigints |
| `numrange` | `Utf8` | Range of numerics (as text) |
| `tsrange` | `Utf8` | Range of timestamps |
| `tstzrange` | `Utf8` | Range of timestamps with tz |
| `daterange` | `Utf8` | Range of dates |

#### Special Types

| PostgreSQL Type | Arrow DataType | Notes |
|-----------------|----------------|-------|
| `oid` | `UInt32` | Object identifier |
| `regclass` | `Utf8` | Relation name |
| `xml` | `Utf8` | XML data |
| `tsvector` | `Utf8` | Text search vector |
| `tsquery` | `Utf8` | Text search query |

#### PostgreSQL Extensions

| Extension Type | Arrow DataType | Notes |
|----------------|----------------|-------|
| `hstore` | `Map(Utf8, Utf8)` | Key-value pairs |
| `ltree` | `Utf8` | Label tree |
| `cube` | `Utf8` | Multi-dimensional cube |

### MySQL Type Mapping

MySQL has both signed and unsigned integer variants, which Arrow handles natively.

#### Numeric Types - Signed

| MySQL Type | Arrow DataType | Range | Notes |
|------------|----------------|-------|-------|
| `TINYINT` | `Int8` | -128 to 127 | 1 byte |
| `SMALLINT` | `Int16` | -32,768 to 32,767 | 2 bytes |
| `MEDIUMINT` | `Int32` | -8M to 8M | 3 bytes, stored as Int32 |
| `INT` / `INTEGER` | `Int32` | -2B to 2B | 4 bytes |
| `BIGINT` | `Int64` | Full 64-bit | 8 bytes |
| `FLOAT` | `Float32` | ~7 digits | IEEE 754 single |
| `DOUBLE` / `REAL` | `Float64` | ~15 digits | IEEE 754 double |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `Decimal128(p, s)` | Exact | Up to 65 digits precision |
| `DECIMAL` (no precision) | `Decimal128(10, 0)` | Exact | Default precision |

#### Numeric Types - Unsigned

| MySQL Type | Arrow DataType | Range | Notes |
|------------|----------------|-------|-------|
| `TINYINT UNSIGNED` | `UInt8` | 0 to 255 | 1 byte |
| `SMALLINT UNSIGNED` | `UInt16` | 0 to 65,535 | 2 bytes |
| `MEDIUMINT UNSIGNED` | `UInt32` | 0 to 16M | 3 bytes, stored as UInt32 |
| `INT UNSIGNED` | `UInt32` | 0 to 4B | 4 bytes |
| `BIGINT UNSIGNED` | `UInt64` | 0 to 18 quintillion | 8 bytes |

**Note on UNSIGNED coercion**: When joining MySQL `INT UNSIGNED` (0 to 4B) with PostgreSQL `integer` (-2B to 2B), we widen to `Int64` to safely hold all possible values from both sources.

#### Character Types

| MySQL Type | Arrow DataType | Notes |
|------------|----------------|-------|
| `CHAR(n)` | `Utf8` | Fixed-length, space-padded |
| `VARCHAR(n)` | `Utf8` | Variable-length with limit |
| `TINYTEXT` | `Utf8` | Up to 255 bytes |
| `TEXT` | `Utf8` | Up to 65KB |
| `MEDIUMTEXT` | `Utf8` | Up to 16MB |
| `LONGTEXT` | `Utf8` | Up to 4GB |
| `ENUM(...)` | `Utf8` + `SemanticType::Categorical` | Enumerated values |
| `SET(...)` | `Utf8` | Comma-separated set values |

#### Binary Types

| MySQL Type | Arrow DataType | Notes |
|------------|----------------|-------|
| `BINARY(n)` | `Binary` | Fixed-length binary |
| `VARBINARY(n)` | `Binary` | Variable-length binary |
| `TINYBLOB` | `Binary` | Up to 255 bytes |
| `BLOB` | `Binary` | Up to 65KB |
| `MEDIUMBLOB` | `Binary` | Up to 16MB |
| `LONGBLOB` | `Binary` | Up to 4GB |
| `BIT(n)` | `Binary` | Bit field |

#### Date/Time Types

| MySQL Type | Arrow DataType | Precision | Notes |
|------------|----------------|-----------|-------|
| `DATE` | `Date32` | Day | '1000-01-01' to '9999-12-31' |
| `TIME` | `Time64(Microsecond)` | μs | '-838:59:59' to '838:59:59' |
| `DATETIME` | `Timestamp(Microsecond, None)` | μs | No timezone |
| `TIMESTAMP` | `Timestamp(Microsecond, Some("UTC"))` | μs | Stored as UTC |
| `YEAR` | `Int16` | Year | 1901 to 2155 |

**Important**: MySQL `TIMESTAMP` is automatically converted to UTC on storage and back to session timezone on retrieval. We always store as UTC in Arrow.

#### Boolean Type

| MySQL Type | Arrow DataType | Notes |
|------------|----------------|-------|
| `BOOLEAN` / `BOOL` | `Boolean` | Alias for TINYINT(1), but mapped to Boolean |
| `TINYINT(1)` | `Boolean` | When used as boolean |

#### JSON Type

| MySQL Type | Arrow DataType | Notes |
|------------|----------------|-------|
| `JSON` | `Utf8` | Stored as validated JSON text |

#### Spatial Types

| MySQL Type | Arrow DataType | Notes |
|------------|----------------|-------|
| `GEOMETRY` | `Binary` | WKB format |
| `POINT` | `Binary` | WKB format |
| `LINESTRING` | `Binary` | WKB format |
| `POLYGON` | `Binary` | WKB format |
| `MULTIPOINT` | `Binary` | WKB format |
| `MULTILINESTRING` | `Binary` | WKB format |
| `MULTIPOLYGON` | `Binary` | WKB format |
| `GEOMETRYCOLLECTION` | `Binary` | WKB format |

### Arrow/Parquet Type Mapping

Arrow is our canonical internal type system. Parquet files use Arrow-compatible types with direct mapping.

#### Arrow Primitive Types (Full Reference)

| Arrow DataType | Parquet Type | Precision | Notes |
|----------------|--------------|-----------|-------|
| `Boolean` | `BOOLEAN` | 1 bit | true/false/null |
| `Int8` | `INT32` + INT_8 | 8 bits | Signed byte |
| `Int16` | `INT32` + INT_16 | 16 bits | Signed short |
| `Int32` | `INT32` | 32 bits | Signed integer |
| `Int64` | `INT64` | 64 bits | Signed long |
| `UInt8` | `INT32` + UINT_8 | 8 bits | Unsigned byte |
| `UInt16` | `INT32` + UINT_16 | 16 bits | Unsigned short |
| `UInt32` | `INT32` + UINT_32 | 32 bits | Unsigned integer |
| `UInt64` | `INT64` + UINT_64 | 64 bits | Unsigned long |
| `Float16` | `FLOAT` + FLOAT16 | ~3 digits | Half precision |
| `Float32` | `FLOAT` | ~7 digits | Single precision |
| `Float64` | `DOUBLE` | ~15 digits | Double precision |
| `Decimal128(p, s)` | `FIXED_LEN_BYTE_ARRAY` + DECIMAL | Exact | p ≤ 38 |
| `Decimal256(p, s)` | `FIXED_LEN_BYTE_ARRAY` + DECIMAL | Exact | p ≤ 76 |

#### Arrow Temporal Types

| Arrow DataType | Parquet Type | Precision | Notes |
|----------------|--------------|-----------|-------|
| `Date32` | `INT32` + DATE | Day | Days since epoch |
| `Date64` | `INT64` + DATE | Millisecond | Milliseconds since epoch |
| `Time32(Second)` | `INT32` + TIME | Seconds | Time of day |
| `Time32(Millisecond)` | `INT32` + TIME | Milliseconds | Time of day |
| `Time64(Microsecond)` | `INT64` + TIME | Microseconds | Time of day |
| `Time64(Nanosecond)` | `INT64` + TIME | Nanoseconds | Time of day |
| `Timestamp(Second, tz)` | `INT64` + TIMESTAMP | Seconds | With optional timezone |
| `Timestamp(Millisecond, tz)` | `INT64` + TIMESTAMP | Milliseconds | With optional timezone |
| `Timestamp(Microsecond, tz)` | `INT64` + TIMESTAMP | Microseconds | With optional timezone |
| `Timestamp(Nanosecond, tz)` | `INT64` + TIMESTAMP | Nanoseconds | With optional timezone |
| `Duration(Second)` | `INT64` | Seconds | Time interval |
| `Duration(Millisecond)` | `INT64` | Milliseconds | Time interval |
| `Duration(Microsecond)` | `INT64` | Microseconds | Time interval |
| `Duration(Nanosecond)` | `INT64` | Nanoseconds | Time interval |
| `Interval(YearMonth)` | `FIXED_LEN_BYTE_ARRAY` | Months | Year-month interval |
| `Interval(DayTime)` | `FIXED_LEN_BYTE_ARRAY` | Days+ms | Day-time interval |
| `Interval(MonthDayNano)` | `FIXED_LEN_BYTE_ARRAY` | Full | Complete interval |

#### Arrow String & Binary Types

| Arrow DataType | Parquet Type | Notes |
|----------------|--------------|-------|
| `Utf8` | `BYTE_ARRAY` + STRING | UTF-8, offset < 2GB |
| `LargeUtf8` | `BYTE_ARRAY` + STRING | UTF-8, offset 64-bit |
| `Binary` | `BYTE_ARRAY` | Variable-length bytes |
| `LargeBinary` | `BYTE_ARRAY` | 64-bit offsets |
| `FixedSizeBinary(n)` | `FIXED_LEN_BYTE_ARRAY` | Fixed n bytes |

#### Arrow Nested Types

| Arrow DataType | Parquet Type | Notes |
|----------------|--------------|-------|
| `List(T)` | Repeated group | Variable-length array |
| `LargeList(T)` | Repeated group | 64-bit offsets |
| `FixedSizeList(T, n)` | Repeated group | Fixed n elements |
| `Struct(fields...)` | Group | Named fields |
| `Map(K, V)` | MAP | Key-value pairs |
| `Union(types...)` | - | Sum type (limited Parquet support) |

#### Parquet Physical vs Logical Types

Parquet stores data in physical types with logical type annotations:

| Physical Type | Logical Type | Arrow Type |
|---------------|--------------|------------|
| `INT32` | `INT(8, signed)` | `Int8` |
| `INT32` | `INT(16, signed)` | `Int16` |
| `INT32` | (none) | `Int32` |
| `INT64` | `INT(64, signed)` | `Int64` |
| `INT32` | `DATE` | `Date32` |
| `INT64` | `TIMESTAMP(isAdjustedToUTC, unit)` | `Timestamp(unit, tz)` |
| `FIXED_LEN_BYTE_ARRAY` | `DECIMAL(precision, scale)` | `Decimal128(p, s)` |
| `BYTE_ARRAY` | `STRING` | `Utf8` |
| `BYTE_ARRAY` | `JSON` | `Utf8` (with semantic) |
| `BYTE_ARRAY` | `UUID` | `FixedSizeBinary(16)` |

**Precision Preservation**: Parquet metadata fully preserves Arrow type information. When reading Parquet files, no type inference is needed - the schema is exact.

### Stripe API Type Mapping

Stripe API returns JSON with specific conventions. We map these to Arrow types with semantic metadata.

#### Core Principles

1. **Amounts are always in cents** (smallest currency unit): `amount: 1999` = $19.99
2. **Timestamps are Unix seconds**: `created: 1672531200` = 2023-01-01T00:00:00Z
3. **IDs are string identifiers**: `id: "ch_1abc..."`, `customer: "cus_xyz..."`
4. **Status fields are categorical**: `status: "succeeded"`, `status: "pending"`

#### Amount/Money Fields

| Stripe Field | Arrow DataType | Semantic Type | Notes |
|--------------|----------------|---------------|-------|
| `amount` | `Int64` | `Money { currency, in_cents: true }` | Always in smallest unit |
| `amount_captured` | `Int64` | `Money { currency, in_cents: true }` | Amount actually captured |
| `amount_refunded` | `Int64` | `Money { currency, in_cents: true }` | Total refunded |
| `fee` | `Int64` | `Money { currency: "USD", in_cents: true }` | Stripe fee (USD) |
| `net` | `Int64` | `Money { currency: "USD", in_cents: true }` | Net amount after fees |
| `unit_amount` | `Int64` | `Money { currency, in_cents: true }` | Price per unit |

**Important**: When comparing Stripe `amount` (cents) with PostgreSQL `money` (dollars), you must use `cents_to_dollars()` or `dollars_to_cents()`:

```sql
-- This will ERROR (semantic type mismatch):
SELECT * FROM stripe.charges c
JOIN postgres.orders o ON c.amount = o.total

-- Correct:
SELECT * FROM stripe.charges c
JOIN postgres.orders o ON cents_to_dollars(c.amount) = o.total
```

#### Identifier Fields

| Stripe Field | Arrow DataType | Semantic Type | Notes |
|--------------|----------------|---------------|-------|
| `id` | `Utf8` | `Identifier` | Primary key, e.g., `ch_1abc...` |
| `customer` | `Utf8` | `Identifier` | Foreign key to customer |
| `payment_intent` | `Utf8` | `Identifier` | Foreign key to payment intent |
| `invoice` | `Utf8` | `Identifier` | Foreign key to invoice |
| `subscription` | `Utf8` | `Identifier` | Foreign key to subscription |
| `source` | `Utf8` | `Identifier` | Payment source ID |

#### Timestamp Fields

| Stripe Field | Arrow DataType | Precision | Notes |
|--------------|----------------|-----------|-------|
| `created` | `Timestamp(Second, Some("UTC"))` | Seconds | Unix timestamp |
| `updated` | `Timestamp(Second, Some("UTC"))` | Seconds | Last update time |
| `period_start` | `Timestamp(Second, Some("UTC"))` | Seconds | Billing period start |
| `period_end` | `Timestamp(Second, Some("UTC"))` | Seconds | Billing period end |
| `trial_start` | `Timestamp(Second, Some("UTC"))` | Seconds | Trial start |
| `trial_end` | `Timestamp(Second, Some("UTC"))` | Seconds | Trial end |
| `canceled_at` | `Timestamp(Second, Some("UTC"))` | Seconds | Cancellation time |

**Note**: Stripe uses second precision. When joining with PostgreSQL (microsecond precision), Arrow auto-converts to the higher precision.

#### Status/Categorical Fields

| Stripe Field | Arrow DataType | Semantic Type | Possible Values |
|--------------|----------------|---------------|-----------------|
| `status` (charge) | `Utf8` | `Categorical` | `succeeded`, `pending`, `failed` |
| `status` (subscription) | `Utf8` | `Categorical` | `active`, `past_due`, `canceled`, `unpaid`, `trialing` |
| `status` (invoice) | `Utf8` | `Categorical` | `draft`, `open`, `paid`, `uncollectible`, `void` |
| `currency` | `Utf8` | `Categorical` | ISO 4217 codes: `usd`, `eur`, `gbp`, ... |
| `payment_method_types` | `List(Utf8)` | - | `["card"]`, `["card", "ideal"]` |

#### Boolean Fields

| Stripe Field | Arrow DataType | Notes |
|--------------|----------------|-------|
| `livemode` | `Boolean` | true = production, false = test |
| `captured` | `Boolean` | Whether charge was captured |
| `paid` | `Boolean` | Whether payment succeeded |
| `refunded` | `Boolean` | Whether charge was refunded |
| `disputed` | `Boolean` | Whether charge is disputed |

#### Nested Objects

| Stripe Field | Arrow DataType | Notes |
|--------------|----------------|-------|
| `metadata` | `Map(Utf8, Utf8)` | Custom key-value pairs |
| `address` | `Struct(...)` | Billing/shipping address |
| `card` | `Struct(...)` | Card details (last4, brand, etc.) |
| `outcome` | `Struct(...)` | Risk/3DS outcome |

#### Stripe Object Type Summary

| Object | Key Fields | Amount Semantic |
|--------|------------|-----------------|
| `charges` | `amount`, `currency`, `status`, `customer` | `in_cents: true` |
| `customers` | `id`, `email`, `created` | N/A |
| `subscriptions` | `id`, `status`, `current_period_start/end` | N/A |
| `invoices` | `amount_due`, `amount_paid`, `currency` | `in_cents: true` |
| `payment_intents` | `amount`, `currency`, `status` | `in_cents: true` |
| `balance_transactions` | `amount`, `fee`, `net`, `currency` | `in_cents: true` |
| `refunds` | `amount`, `currency`, `charge` | `in_cents: true` |
| `products` | `id`, `name`, `active` | N/A |
| `prices` | `unit_amount`, `currency`, `product` | `in_cents: true` |

### CSV Type Inference

CSV files have no schema - all values are strings. We must infer types by scanning the data.

#### Inference Strategy: Most Permissive Types

To prevent breakage when data grows, we always choose the **widest possible type**:

| Inferred Type | Arrow DataType | Rationale |
|---------------|----------------|-----------|
| Integer | `Int64` | Not `Int8`/`Int32` - data may grow beyond initial range |
| Decimal | `Float64` | Not `Float32` - preserve precision for financial data |
| Boolean | `Boolean` | Only when all values are `true`/`false`/`1`/`0` |
| Date | `Date32` | When all values match date patterns |
| Timestamp | `Timestamp(Microsecond, None)` | When all values match datetime patterns |
| String | `Utf8` | Fallback for any non-parseable values |

**Example**: If a column has values `1, 2, 3, 255`, we use `Int64` (not `UInt8`), because:
- Future rows might contain `256` or negative numbers
- Using `UInt8` would break on `256`
- Using `Int64` handles virtually any integer growth

#### Full-Scan Inference Process

During indexing, we scan **all rows** (not just a sample):

```
1. For each column, track:
   - null_count: How many empty/null values
   - bool_valid: Can all values parse as boolean?
   - int_valid: Can all values parse as integer?
   - float_valid: Can all values parse as float?
   - date_valid: Can all values match date patterns?
   - datetime_valid: Can all values match datetime patterns?
   - sample_values: First N non-null values (for UI preview)

2. After scanning all rows, select type:
   if all values are empty → Utf8 (nullable)
   else if bool_valid → Boolean
   else if int_valid → Int64
   else if float_valid → Float64
   else if datetime_valid → Timestamp(Microsecond, None)
   else if date_valid → Date32
   else → Utf8
```

#### Type Detection Patterns

| Type | Recognized Patterns |
|------|---------------------|
| **Boolean** | `true`, `false`, `TRUE`, `FALSE`, `1`, `0`, `yes`, `no`, `Y`, `N` |
| **Integer** | `-?[0-9]+` (no decimal point, no exponent) |
| **Float** | `-?[0-9]+\.[0-9]+`, `-?[0-9]+[eE][+-]?[0-9]+` |
| **Date** | `YYYY-MM-DD`, `MM/DD/YYYY`, `DD-Mon-YYYY` |
| **Timestamp** | `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`, ISO 8601 variants |

#### Null/Empty Handling

| CSV Value | Interpretation |
|-----------|----------------|
| Empty string `""` | NULL |
| Whitespace-only | NULL |
| `NULL` (literal) | NULL |
| `NA`, `N/A` | NULL |
| `NaN` (for numerics) | NULL or NaN depending on context |

#### User Override in UI

Since inference can be wrong, users can review and override types:

```
┌─────────────────────────────────────────────────────────────────┐
│ CSV Schema: sales_data.csv                                      │
├─────────────────────────────────────────────────────────────────┤
│ Column          │ Inferred Type │ Sample Values      │ Override │
├─────────────────┼───────────────┼────────────────────┼──────────┤
│ order_id        │ Int64         │ 1001, 1002, 1003   │ [▾]      │
│ customer_name   │ String        │ "Alice", "Bob"     │ [▾]      │
│ amount          │ Float64       │ 99.99, 149.50      │ [▾]      │
│ order_date      │ Date32        │ 2024-01-15, ...    │ [▾]      │
│ is_shipped      │ Boolean       │ true, false        │ [▾]      │
│ notes           │ String        │ "Rush order", ""   │ [▾]      │
└─────────────────────────────────────────────────────────────────┘
                                              [ Save Schema ]
```

Override options:
- Force to `String` (always safe)
- Force to specific numeric type (`Int32`, `Int64`, `Float64`)
- Force to temporal type (`Date32`, `Timestamp`)
- Mark as `Identifier` (semantic type)

#### Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| No schema declaration | Must infer from data | Full-scan inference |
| Ambiguous values | `"123"` - number or ID? | User override in UI |
| Date format variance | `01/02/03` - which format? | Try multiple patterns |
| Currency symbols | `$1,234.56` | Strip symbols before parsing |
| Thousands separators | `1,000,000` | Handle locale-specific formats |
| Scientific notation | `1.23E+10` | Parse as Float64 |

### Type Compatibility Matrix

This matrix shows how types from different sources can be combined:

#### Legend

- ✅ **Auto**: Automatic coercion, no user action needed
- ⚠️ **Warn**: Auto-coerce with warning (potential precision loss)
- 🔄 **Cast**: Requires explicit `CAST(x AS type)`
- 💱 **Func**: Requires conversion function (e.g., `cents_to_dollars()`)
- ❌ **Error**: Incompatible types

#### Numeric Type Coercion Matrix

| From ↓ To → | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 | Decimal |
|-------------|------|-------|-------|-------|-------|--------|--------|--------|---------|---------|---------|
| **Int8** | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ |
| **Int16** | 🔄 | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ |
| **Int32** | 🔄 | 🔄 | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ |
| **Int64** | 🔄 | 🔄 | 🔄 | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ |
| **UInt8** | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **UInt16** | ⚠️ | ⚠️ | ✅ | ✅ | 🔄 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **UInt32** | ⚠️ | ⚠️ | ⚠️ | ✅ | 🔄 | 🔄 | ✅ | ✅ | ⚠️ | ✅ | ✅ |
| **UInt64** | ⚠️ | ⚠️ | ⚠️ | ⚠️ | 🔄 | 🔄 | 🔄 | ✅ | ⚠️ | ⚠️ | ✅ |
| **Float32** | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | ✅ | ✅ | ⚠️ |
| **Float64** | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | ✅ | ⚠️ |
| **Decimal** | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | ⚠️ | ⚠️ | ✅* |

*Decimal to Decimal: Auto if target precision ≥ source precision, else ⚠️

#### Timestamp Precision Coercion

| From ↓ To → | Second | Millisecond | Microsecond | Nanosecond |
|-------------|--------|-------------|-------------|------------|
| **Second** | ✅ | ✅ | ✅ | ✅ |
| **Millisecond** | ⚠️ | ✅ | ✅ | ✅ |
| **Microsecond** | ⚠️ | ⚠️ | ✅ | ✅ |
| **Nanosecond** | ⚠️ | ⚠️ | ⚠️ | ✅ |

**Note**: Precision loss (μs → s) triggers a warning. The value is truncated, not rounded.

#### Timestamp Timezone Coercion

| From ↓ To → | No TZ | With TZ |
|-------------|-------|---------|
| **No TZ** | ✅ | 🔄 (ambiguous: what timezone?) |
| **With TZ** | ⚠️ (strips TZ info) | ✅ (converts to target TZ) |

#### String Conversions

| From ↓ To → | Utf8 | Int64 | Float64 | Date32 | Timestamp | Boolean |
|-------------|------|-------|---------|--------|-----------|---------|
| **Utf8** | ✅ | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 |
| **Int64** | 🔄 | ✅ | - | - | - | - |
| **Float64** | 🔄 | - | ✅ | - | - | - |
| **Date32** | 🔄 | - | - | ✅ | - | - |
| **Timestamp** | 🔄 | - | - | - | ✅ | - |
| **Boolean** | 🔄 | - | - | - | - | ✅ |

**Note**: All `String → X` conversions require explicit `CAST` because they can fail at runtime.

#### Semantic Type Coercion

| From ↓ To → | Money(cents) | Money(dollars) | Percentage(0-1) | Percentage(0-100) |
|-------------|--------------|----------------|-----------------|-------------------|
| **Money(cents)** | ✅ | 💱 `cents_to_dollars()` | ❌ | ❌ |
| **Money(dollars)** | 💱 `dollars_to_cents()` | ✅ | ❌ | ❌ |
| **Percentage(0-1)** | ❌ | ❌ | ✅ | 💱 `* 100` |
| **Percentage(0-100)** | ❌ | ❌ | 💱 `/ 100` | ✅ |

#### Cross-Source Type Examples

| Scenario | PostgreSQL | MySQL | Stripe | CSV | Coercion |
|----------|------------|-------|--------|-----|----------|
| User ID | `bigint` | `BIGINT UNSIGNED` | `Utf8` (id) | `Int64` | 🔄 All to `Utf8` or all to `Int64` |
| Amount | `money` | `DECIMAL(10,2)` | `Int64` (cents) | `Float64` | 💱 `cents_to_dollars()` for Stripe |
| Timestamp | `timestamptz` (μs) | `TIMESTAMP` (μs) | `Int64` (sec) | `Timestamp` | ✅ Auto (precision alignment) |
| Status | `text` | `ENUM` | `Utf8` | `Utf8` | ✅ Auto (all strings) |

#### Error Messages

When coercion fails, we provide actionable error messages:

```sql
-- Query:
SELECT * FROM stripe.charges c
JOIN postgres.orders o ON c.amount = o.total

-- Error:
ERROR: Cannot compare stripe.charges.amount (Money, cents) with postgres.orders.total (Money, dollars)
HINT: Use cents_to_dollars(stripe.charges.amount) or dollars_to_cents(postgres.orders.total)
```

```sql
-- Query:
SELECT * FROM csv.users u
JOIN postgres.customers c ON u.signup_date = c.created_at

-- Warning:
WARNING: Comparing csv.users.signup_date (Date32) with postgres.customers.created_at (Timestamp)
         Date will be treated as midnight UTC. Use CAST if you need different behavior.
```

---


