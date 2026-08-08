# Realistic Data Generation for Reiver

## Overview

This script generates realistic production-like observability data to test the Reiver UI with data that resembles what a real company would see.

## What It Generates

### 📊 Volume (Over 24 hours)
- **~200,000+ traces** across 10 microservices
- **~500,000+ log entries** (info, warning, error levels)
- **~5,000+ exceptions** (various error types)
- **~10,000+ metric data points**

### 🏢 Realistic Scenarios

#### Microservices Architecture
Simulates 10 interconnected services:
- `api-gateway` - Main entry point
- `auth-service` - Authentication & authorization
- `user-service` - User management
- `payment-service` - Payment processing
- `order-service` - Order management
- `inventory-service` - Inventory tracking
- `notification-service` - Email/SMS/Push notifications
- `analytics-service` - Analytics & reporting
- `search-service` - Product search
- `recommendation-service` - Product recommendations

#### Error Scenarios
Realistic production errors:
- **TypeError**: "Cannot read property 'id' of undefined"
- **DatabaseConnectionError**: "Connection pool exhausted"
- **RateLimitError**: "Rate limit exceeded"
- **PaymentProcessingError**: "Payment gateway timeout"
- **AuthenticationError**: "JWT token expired"
- **RedisConnectionError**: "Redis connection lost"
- **ElasticsearchError**: "Search query timeout"
- **KafkaProducerError**: "Failed to produce message"

#### Traffic Patterns
Simulates realistic daily patterns:
- **Peak hours (9 AM - 5 PM)**: 2-3x normal traffic
- **Off-hours (12 AM - 6 AM)**: 30-70% normal traffic
- **Moderate hours**: Regular traffic
- **Error spikes**: Random 5% chance per 5-minute interval

#### Distributed Tracing
Each trace includes:
- Root span (HTTP request)
- Database query span (PostgreSQL)
- Cache lookup span (Redis, 70% of requests)
- HTTP call to downstream service (50% of requests)
- Realistic durations and status codes

#### Log Variety
Diverse log messages:
- User authentication logs
- Payment processing logs
- Order creation logs
- Email notification logs
- Cache hit/miss logs
- Database query performance logs
- API request logs
- Background job logs
- Rate limit warnings
- Inventory updates

#### Metrics
System and application metrics:
- `http_requests_total` (counter)
- `http_request_duration_ms` (gauge)
- `active_connections` (gauge)
- `cpu_usage_percent` (gauge)
- `memory_usage_mb` (gauge)
- `error_rate_percent` (gauge)

## Installation

### Prerequisites
- Python 3.7+
- `requests` library

```bash
pip install requests
```

## Usage

### Basic Usage
```bash
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY>
```

### With Custom Time Range
```bash
# Generate data for last 48 hours
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 48

# Generate data for last 7 days
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 168
```

### Example
```bash
python scripts/generate_realistic_data.py proj_abc123 sk_live_xyz789 24
```

## Getting Your Project ID and API Key

### Option 1: Via UI
1. Log in to Reiver
2. Go to your project
3. Navigate to **Settings** → **API Keys**
4. Copy your **Project ID** and **API Key**

### Option 2: Via Database
```sql
-- Get project ID
SELECT id, name FROM projects;

-- Get API key
SELECT key FROM project_api_keys WHERE project_id = '<PROJECT_ID>';
```

## Expected Output

```
🚀 Generating realistic workload for 24 hours...
📊 Project ID: proj_abc123
🔑 API Key: sk_live_xy...

⏳ Progress: 45.3% | Exceptions: 2,341 | Traces: 98,432 | Logs: 234,567 | Metrics: 4,320

✅ Data generation complete!
📈 Summary:
   - Exceptions: 5,234
   - Traces: 204,876
   - Logs: 512,345
   - Metrics: 10,080

🌐 View your data at: http://localhost:3000/projects/proj_abc123/observability
```

## Performance Notes

### Generation Speed
- **Fast Mode** (~5 min for 24 hours): Adjust `time.sleep(0.1)` to `time.sleep(0.01)`
- **Normal Mode** (~15 min for 24 hours): Default
- **Careful Mode** (~30 min for 24 hours): Use if server is under load

### Server Load
The script includes a small delay (`time.sleep(0.1)`) to avoid overwhelming the server. If you're running on a powerful machine, you can reduce this delay.

### Data Volume Control
To generate less data, reduce the time range:
```bash
# Generate data for last 6 hours
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 6

# Generate data for last 1 hour
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 1
```

## Testing Scenarios

### Test Unified Observability Page
1. Generate data: `python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 24`
2. Navigate to: `/projects/<PROJECT_ID>/observability`
3. Test features:
   - Switch between tabs: All Events | Errors | Traces | Logs
   - Apply filters: Time range, Severity, Status, Service
   - Expand rows inline
   - Click "Resolve" on errors
   - Search for specific messages

### Test Error Spikes
The script automatically generates error spikes (5% chance per 5-minute interval). Look for:
- Sudden increase in error count
- Multiple related errors in short time
- Impact on trace success rate

### Test Service Dependencies
Traces show realistic service-to-service communication:
- Root span: API Gateway
- Child spans: Database, Redis, Downstream services
- Use the trace detail page to visualize the waterfall

### Test Time-Based Patterns
The data follows realistic daily patterns:
- **Morning** (6-9 AM): Traffic ramps up
- **Business hours** (9 AM - 5 PM): Peak traffic
- **Evening** (5-9 PM): Traffic decreases
- **Night** (9 PM - 6 AM): Low traffic

## Customization

### Add More Services
Edit `SERVICES` list in the script:
```python
SERVICES = [
    "api-gateway",
    "auth-service",
    # Add your services here
    "billing-service",
    "reporting-service",
]
```

### Add Custom Error Scenarios
Edit `ERROR_SCENARIOS` list:
```python
ERROR_SCENARIOS = [
    {
        "type": "CustomError",
        "message": "Your error message here",
        "service": "your-service",
        "file": "src/path/to/file.js",
        "line": 123,
        "severity": "error",
    },
]
```

### Add Custom Log Messages
Edit `LOG_TEMPLATES` list:
```python
LOG_TEMPLATES = [
    "Your log message with {variable}",
]
```

### Adjust Traffic Distribution
Modify traffic multipliers in `generate_realistic_workload()`:
```python
# Peak hours: 9 AM - 5 PM
if 9 <= hour <= 17:
    traffic_multiplier = random.uniform(5.0, 10.0)  # Even higher traffic
```

### Adjust Error Rate
Modify error probability:
```python
# Traces
is_error = random.random() < 0.10  # 10% error rate instead of 5%

# Error spikes
error_spike = random.random() < 0.10  # 10% chance instead of 5%
```

## Troubleshooting

### Connection Errors
```
Error sending exception: Connection refused
```
**Solution**: Ensure Reiver server is running on `http://localhost:3000`

### Authentication Errors
```
401 Unauthorized
```
**Solution**: Verify your API key is correct and active

### Timeout Errors
```
Timeout error after 5s
```
**Solution**: Increase timeout or reduce data generation speed

### Rate Limiting
If you hit rate limits, the script will continue but some data may be dropped. Check your rate limit configuration.

## Advanced Usage

### Generate Data for Multiple Projects
```bash
# Project 1
python scripts/generate_realistic_data.py proj_1 key_1 24

# Project 2
python scripts/generate_realistic_data.py proj_2 key_2 24
```

### Generate Continuous Data
For ongoing testing, run the script periodically:
```bash
# Generate 1 hour of data every hour
while true; do
    python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 1
    sleep 3600
done
```

### Generate Historical Data
```bash
# Generate data for last 7 days
python scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> 168
```

## What to Test After Generation

### ✅ Observability Page
- [ ] All Events tab shows mixed data types
- [ ] Errors tab shows only exceptions
- [ ] Traces tab shows only traces
- [ ] Logs tab shows only logs
- [ ] Tab counts are accurate
- [ ] Inline expansion works
- [ ] Quick actions work (Resolve, Copy)

### ✅ Filters
- [ ] Time range filter works (15m, 1h, 24h, 7d, 30d)
- [ ] Severity filter works (error, warning, info)
- [ ] Status filter works (unresolved, resolved, ignored)
- [ ] Service filter works
- [ ] Search filter works
- [ ] Active filters display correctly
- [ ] Clear filters works

### ✅ Exceptions Page
- [ ] Shows exception groups
- [ ] Inline expansion works
- [ ] Resolve action works
- [ ] Sorting works (count, last_seen, etc.)
- [ ] Pagination works

### ✅ Performance
- [ ] Page loads in < 2 seconds
- [ ] Filters apply in < 500ms
- [ ] Tab switching is instant (client-side)
- [ ] Inline expansion is instant

### ✅ Data Quality
- [ ] Exceptions have realistic stack traces
- [ ] Traces have realistic spans
- [ ] Logs have contextual data
- [ ] Timestamps are correct
- [ ] Service names are correct

## Next Steps

After generating data:
1. **Test the Observability page** - Main feature
2. **Test filtering** - Ensure all filters work
3. **Test inline expansion** - Click rows to expand
4. **Test performance** - Should handle 100k+ events
5. **Test error resolution** - Mark errors as resolved
6. **Test search** - Search across all events

Enjoy testing! 🚀
