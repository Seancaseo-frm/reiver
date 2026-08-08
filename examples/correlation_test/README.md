# Reiver Correlation Test

This integration test application creates correlated traces, spans, logs, and errors to verify that the trace-log-error correlation feature works correctly in Reiver.

## What It Does

The test simulates a typical e-commerce order processing flow that fails during payment:

```
POST /api/orders (root span)
├── validate_order_input (span)
├── fetch_user_data (span)
│   └── SELECT FROM orders (DB span)
└── process_payment (span)
    └── HTTP GET payment-service (HTTP span)
        └── PaymentError (ERROR!)
```

All spans, logs, and the error share the same `trace_id`, enabling you to verify correlation in the UI.

## Prerequisites

1. Reiver backend running (default: `http://localhost:3000`)
2. A valid API key from Reiver (from `project_keys` table)

## Usage

```bash
# Navigate to the test directory
cd examples/correlation_test

# Run with defaults (uses same credentials as generate_realistic_data.py)
cargo run

# With custom API key
cargo run -- --api-key YOUR_API_KEY

# With custom API URL
cargo run -- --api-url http://your-reiver-server:3000

# With custom service name
cargo run -- --service my-test-service

# All options
cargo run -- --api-key YOUR_API_KEY --project-id YOUR_PROJECT_ID --api-url http://localhost:3000
```

### Getting Your API Key

```sql
-- Query your database for API keys
SELECT key, project_id FROM project_keys;
```

The default values match those in `scripts/generate_realistic_data.py`:
- Project ID: `2c60e43d-e9c0-4275-8091-5387b75622bc`
- API Key: `RzohwTxWGVVM8Vg54ehJulN6AkQz0iJn`

## Example Output

```
🔧 Configuration:
   API URL: http://localhost:3000
   Service: correlation-test-service
   Project ID: 2c60e43d-e9c0-4275-8091-5387b75622bc
   API Key: RzohwTxWGV...

✓ OpenTelemetry tracer initialized

🚀 Starting correlation test scenario...

📍 Created trace:
   trace_id: a1b2c3d4e5f6789012345678901234ab
   root_span_id: 12345678abcdef01
   ✓ Sent log: Received POST /api/orders request
   ✓ Sent log: Validating order input
   ✓ Sent DB query span and log
   ✓ Sent log: Processing payment
   ✓ Sent HTTP call span and log
   ✓ Sent log: Payment error
   ✓ Sent error: PaymentError with trace_id correlation
   ✓ Sent log: Order processing failed

✅ Test scenario completed!

📊 Summary:
   - 1 trace with multiple spans
   - 7 logs with trace_id correlation
   - 1 error with trace_id correlation

🔍 To verify in the UI:
   URL: http://localhost:3000/projects/2c60e43d-e9c0-4275-8091-5387b75622bc/errors
   1. Go to Errors page and find 'PaymentError'
   2. Click 'View Logs' - should show 7 correlated logs
   3. Click 'View Trace' - should show the trace waterfall
   4. All should share trace_id: a1b2c3d4e5f6789012345678901234ab
```

## Verifying Correlation in the UI

After running the test:

### 1. Check Errors Page
- Navigate to your project's Errors page
- Find the "PaymentError" error
- Note the trace_id in the error details

### 2. View Correlated Logs
- Click "View Logs" button on the error
- You should see 7 logs that share the same trace_id
- The Logs page should show a blue "Trace-correlated logs" indicator
- All logs should be from the same request flow

### 3. View Trace
- Click "View Trace" button on the error
- You should see a waterfall view with:
  - Root span: `POST /api/orders`
  - Child spans: `validate_order_input`, `fetch_user_data`, `process_payment`
  - Grandchild spans: DB query, HTTP call
- The trace should show the exception badge on the `process_payment` span

### 4. Navigate from Trace to Logs
- From the trace detail page, you can see associated logs
- Clicking on any span should show logs emitted within that span's context

## Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Test Application                          │
├──────────────────────────────────────────────────────────────┤
│  OpenTelemetry Tracer                                        │
│  ├── Creates spans with trace_id                             │
│  └── Exports via OTLP to /v1/traces                         │
│                                                              │
│  Log Sender                                                  │
│  ├── Gets trace_id/span_id from OTel context                │
│  └── Sends to /api/logs/ingest with trace correlation       │
│                                                              │
│  Error Sender                                                │
│  ├── Gets trace_id/span_id from OTel context                │
│  └── Sends to /api/v1/exceptions with trace correlation     │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│                   Reiver Backend                          │
├──────────────────────────────────────────────────────────────┤
│  /v1/traces → spans table (ClickHouse)                       │
│  /api/logs/ingest → unstructured_logs (ClickHouse)           │
│  /api/v1/exceptions → error_traces (PostgreSQL) +            │
│                       exceptions table (ClickHouse)          │
│                                                              │
│  Correlation: error_traces links error_id to trace_id        │
│               logs have trace_id column for filtering        │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│                   Reiver UI                                │
├──────────────────────────────────────────────────────────────┤
│  Error Detail Page:                                          │
│  ├── Shows error with trace_id                              │
│  ├── "View Logs" → Logs page filtered by trace_id           │
│  └── "View Trace" → Trace waterfall page                    │
│                                                              │
│  Logs Page:                                                  │
│  └── ?trace_id=xxx filter shows correlated logs             │
│                                                              │
│  Trace Page:                                                 │
│  └── Shows exception badges on spans with errors            │
└──────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### No logs appear
- Check that the project key is correct
- Verify the API URL is accessible
- Check Reiver backend logs for errors

### Trace not appearing
- OTLP export might be delayed; wait a few seconds
- Check that the `/v1/traces` endpoint is enabled
- Verify the project key header is being sent

### Error appears but "No Correlated Logs"
- The error's trace_id might not match any logs
- Verify both error and logs were sent successfully
- Check that the trace_id in the error matches the logs
