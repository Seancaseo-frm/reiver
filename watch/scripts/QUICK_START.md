# Quick Start: Generate Test Data

## TL;DR

```bash
# Interactive mode (easiest)
./scripts/generate_test_data.sh

# Direct mode
./scripts/generate_test_data.sh proj_abc123 sk_live_xyz789 24
```

## Step-by-Step Guide

### 1. Get Your Credentials

**Via UI:**
1. Log in to Reiver
2. Go to **Settings** → **API Keys**
3. Copy your **Project ID** and **API Key**

**Via Database:**
```sql
-- Get project ID
SELECT id, name FROM projects;

-- Get API key
SELECT key FROM project_api_keys WHERE project_id = 'your-project-id';
```

### 2. Run the Script

**Option A: Interactive Mode** (Recommended)
```bash
./scripts/generate_test_data.sh
```

The script will ask you for:
- Project ID
- API Key
- Hours of data to generate (default: 24)

**Option B: Direct Mode**
```bash
./scripts/generate_test_data.sh <PROJECT_ID> <API_KEY> [HOURS]
```

Example:
```bash
./scripts/generate_test_data.sh proj_abc123 sk_live_xyz789 24
```

**Option C: Python Directly**
```bash
python3 scripts/generate_realistic_data.py <PROJECT_ID> <API_KEY> [HOURS]
```

### 3. Wait for Completion

**Expected time:**
- 1 hour of data: ~1 minute
- 6 hours of data: ~5 minutes
- 24 hours of data: ~15 minutes
- 7 days of data: ~1.5 hours

**Progress indicator:**
```
⏳ Progress: 45.3% | Exceptions: 2,341 | Traces: 98,432 | Logs: 234,567 | Metrics: 4,320
```

### 4. View Your Data

Open your browser to:
```
http://localhost:3000/projects/<PROJECT_ID>/observability
```

## What Gets Generated

### Volume (24 hours)
- **~200,000 traces** (distributed across 10 services)
- **~500,000 logs** (info, warning, error)
- **~5,000 exceptions** (realistic error scenarios)
- **~10,000 metrics** (CPU, memory, HTTP, etc.)

### Services
- api-gateway
- auth-service
- user-service
- payment-service
- order-service
- inventory-service
- notification-service
- analytics-service
- search-service
- recommendation-service

### Error Types
- TypeError
- ValidationError
- DatabaseConnectionError
- RateLimitError
- PaymentProcessingError
- AuthenticationError
- NotFoundError
- RedisConnectionError
- ElasticsearchError
- KafkaProducerError

### Traffic Patterns
- **Peak hours** (9 AM - 5 PM): 2-3x normal traffic
- **Off-hours** (12 AM - 6 AM): 30-70% traffic
- **Error spikes**: Random occurrences (5% chance per 5 min)

## Common Use Cases

### Quick Test (1 hour)
```bash
./scripts/generate_test_data.sh proj_abc123 sk_live_xyz789 1
```

### Full Day Test (24 hours)
```bash
./scripts/generate_test_data.sh proj_abc123 sk_live_xyz789 24
```

### Weekly Test (7 days)
```bash
./scripts/generate_test_data.sh proj_abc123 sk_live_xyz789 168
```

### Multiple Projects
```bash
# Project 1
./scripts/generate_test_data.sh proj_1 key_1 24

# Project 2
./scripts/generate_test_data.sh proj_2 key_2 24
```

## Testing Checklist

After generation, test these features:

### ✅ Observability Page
- [ ] All Events tab shows all data types
- [ ] Errors tab shows only exceptions
- [ ] Traces tab shows only traces
- [ ] Logs tab shows only logs
- [ ] Inline expansion works
- [ ] Quick actions work (Resolve, Copy)

### ✅ Filters
- [ ] Time range (15m, 1h, 24h, 7d, 30d)
- [ ] Severity (error, warning, info)
- [ ] Status (unresolved, resolved, ignored)
- [ ] Service filter
- [ ] Search filter

### ✅ Performance
- [ ] Page loads < 2 seconds
- [ ] Filters apply < 500ms
- [ ] Inline expansion is instant

## Troubleshooting

### "Python 3 not found"
```bash
# macOS
brew install python3

# Ubuntu/Debian
sudo apt-get install python3

# Fedora/RHEL
sudo dnf install python3
```

### "requests library not found"
```bash
pip3 install requests
```

### "Connection refused"
Make sure Reiver server is running:
```bash
# Start the server
cargo run
```

### "401 Unauthorized"
Check your API key is correct and active.

### "Script permission denied"
```bash
chmod +x scripts/generate_test_data.sh
```

## Need Help?

- **Full documentation**: [README_DATA_GENERATION.md](README_DATA_GENERATION.md)
- **Script source**: [generate_realistic_data.py](generate_realistic_data.py)
- **Customization**: Edit `ERROR_SCENARIOS`, `SERVICES`, or `LOG_TEMPLATES` in the Python script

## Example Output

```
🚀 Generating realistic workload for 24 hours...
📊 Project ID: proj_abc123
🔑 API Key: sk_live_xy...

⏳ Progress: 100.0% | Exceptions: 5,234 | Traces: 204,876 | Logs: 512,345 | Metrics: 10,080

✅ Data generation complete!
📈 Summary:
   - Exceptions: 5,234
   - Traces: 204,876
   - Logs: 512,345
   - Metrics: 10,080

🌐 View your data at: http://localhost:3000/projects/proj_abc123/observability
```

Happy testing! 🚀
